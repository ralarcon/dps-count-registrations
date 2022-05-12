using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Common.Exceptions;
using Microsoft.Azure.Devices.Provisioning.Client;
using Microsoft.Azure.Devices.Provisioning.Client.Transport;
using Microsoft.Azure.Devices.Provisioning.Service;
using Microsoft.Azure.Devices.Shared;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace dps_count_records;

//USE THIS CLASS TO CREATE ENROLLMENT GROUPS AND DEVICE REGISTRATIONS FOR TESTING PURPOSES

//SAMPLE USAGE FROM Program.cs:
//
//  EnrollmentTestHelpers testHelper = new EnrollmentTestHelpers(psc, config);
//  await testHelper.RemoveSampleEnrollmentGroups(
//      groupNamePrefix: "test-eg",
//      enrollmentGroupsCount: 10);
//
//  await testHelper.CreateSampleEnrollmentGroupsWithDevices(
//      groupNamePrefix: "test-eg",
//      enrollmentGroupsCount: 10,
//      provisionDevicesCount: 100).ConfigureAwait(false);

public class EnrollmentTestHelpers
{
    const int MAX_PARALLELISM = 10;

    private readonly ProvisioningServiceClient _psc;
    private readonly string _dpsEndpoint;
    private readonly string _dpsScopeId;
    private readonly IConfiguration _config;
    private readonly RegistryManager _iotHubRegistry;
    public EnrollmentTestHelpers(ProvisioningServiceClient psc, IConfiguration config)
    {
        _psc = psc;
        _config = config;
        _dpsEndpoint = _config.GetValue<string>("Settings:Dps:Endpoint");
        _dpsScopeId = _config.GetValue<string>("Settings:Dps:IdScope");
        _iotHubRegistry = RegistryManager.CreateFromConnectionString(config.GetValue<string>("Settings:IotHub:ConnectionString"));

    }
    public async Task CreateSampleEnrollmentGroupsWithDevices(string groupNamePrefix, int enrollmentGroupsCount, int provisionDevicesCount)
    {
        for (int i = 0; i < enrollmentGroupsCount; i++)
        {
            await CreateEnrollmentGroupAsync($"{groupNamePrefix}-{i.ToString("000")}", provisionDevicesCount).ConfigureAwait(false);
        }
    }

    public async Task RemoveSampleEnrollmentGroups(string groupNamePrefix, int enrollmentGroupsCount)
    {
        for (int i = 0; i < enrollmentGroupsCount; i++)
        {
            await RemoveEnrollmentGroupAsync($"{groupNamePrefix}-{i.ToString("000")}").ConfigureAwait(false);
        }
    }

    public async Task CreateEnrollmentGroupAsync(string enrollmentGroupId, int? provisionDevicesCount = null)
    {
        Console.WriteLine("\nPreparing Enrollment Group...");

        string primaryKey = CreateSymetricKey($"{enrollmentGroupId}-myKey1-test", "testing");
        string secondaryKey = CreateSymetricKey($"{enrollmentGroupId}-myKey2-test", "testing");
        Attestation attestation = new SymmetricKeyAttestation(primaryKey, secondaryKey);

        EnrollmentGroup enrollmentGroup = await PerpareEnrollmentGroupAsync(enrollmentGroupId, attestation).ConfigureAwait(false);

        await _psc.CreateOrUpdateEnrollmentGroupAsync(enrollmentGroup).ConfigureAwait(false);

        if (provisionDevicesCount is not null)
        {
            await ProvisionDevicesAsync(enrollmentGroupId, provisionDevicesCount.Value, primaryKey).ConfigureAwait(false);
        }
    }

    public async Task RemoveEnrollmentGroupAsync(string enrollmentGroupId)
    {
        //Removes all device registrations belonging to the enrolloment group
        //Removes all devices from IoT Hub
        //Removes the enrollment group from DPS

        try
        {
            await _psc.GetEnrollmentGroupAsync(enrollmentGroupId);
        }
        catch (ProvisioningServiceClientHttpException ex) when (!ex.IsTransient && ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            Console.WriteLine($"Enrollment group {enrollmentGroupId} not found.");
            return;
        }
        bool removeEnrollmentGroup = true;
        using (Query registrationStateQuery = _psc.CreateEnrollmentGroupRegistrationStateQuery(
                    new QuerySpecification($"SELECT * FROM enrollmentGroups"), enrollmentGroupId))
        {
            Console.WriteLine($"Removing device registrations and devices for enrollment group {enrollmentGroupId}...");

            while (registrationStateQuery.HasNext())
            {
                QueryResult queryResult = await registrationStateQuery.NextAsync().ConfigureAwait(false);
                removeEnrollmentGroup = removeEnrollmentGroup && await DeleteRegistrations(queryResult).ConfigureAwait(false);
            }
        }
        if (removeEnrollmentGroup)
        {
            await _psc.DeleteEnrollmentGroupAsync(enrollmentGroupId).ConfigureAwait(false);
            Console.WriteLine($"\tSuccessfully removed the enrollment group {enrollmentGroupId} and its registrations and devices.");
        }
        else
        {
            Console.WriteLine($"\tThere has been errors removing registrations and/or devices. The enrollment {enrollmentGroupId} has not been deleted.");
        }
    }

    private async Task<bool> DeleteRegistrations(QueryResult queryResult)
    {
        bool success = true;
        int registrations = 0;
        int devices = 0;

        ParallelOptions parallelOptions = new()
        {
            MaxDegreeOfParallelism = MAX_PARALLELISM
        };

        await Parallel.ForEachAsync(queryResult.Items.Select(i => i as DeviceRegistrationState), parallelOptions, async (deviceRegistration, token) =>
        {
            if (deviceRegistration != null)
            {
                if (await SafeDeleteRegistrationAsync(deviceRegistration).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref registrations);
                }
                else
                {
                    success = false;
                }

                if (await SafeRemoveDeviceAsync(deviceRegistration.DeviceId).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref devices);
                }
                else
                {
                    success = false;
                }
            }
        });
        Console.WriteLine($"\tRegistrations Deleted: {registrations}. Devices Removed: {devices}.");
        return success;
    }

    private async Task<bool> SafeDeleteRegistrationAsync(DeviceRegistrationState deviceRegistration)
    {
        try
        {
            await _psc.DeleteDeviceRegistrationStateAsync(deviceRegistration).ConfigureAwait(false);
            return true;
        }
        catch (ProvisioningServiceClientHttpException ex) when (ex.IsTransient && ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
        {
            Console.WriteLine($"\tTransient error trying removing device registration. {ex.ErrorMessage}.");
            if (ex.Fields.Keys.Contains("Retry-After") && int.TryParse(ex.Fields["Retry-After"], out var delaySeconds))
            {
                Console.WriteLine($"\tOperation will retry after {delaySeconds} seconds.");
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                await _psc.DeleteDeviceRegistrationStateAsync(deviceRegistration).ConfigureAwait(false);
                return true;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\tUnable to remove device registration {deviceRegistration.RegistrationId} from DPS. Error {ex.Message}");
        }
        return false;
    }
    private async Task<bool> SafeRemoveDeviceAsync(string deviceId)
    {
        try
        {
            await _iotHubRegistry.RemoveDeviceAsync(deviceId).ConfigureAwait(false);
            return true;
        }
        catch (ThrottlingException ex)
        {
            Console.WriteLine($"\tTransient error trying to remove registrations and devices. Message {ex.Message}. Delaying 1 minute to continue.");
            await Task.Delay(60000);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\tUnable to remove the device {deviceId} from IoT Hub. Error {ex.Message}");
        }
        return false;
    }

    private async Task<EnrollmentGroup> PerpareEnrollmentGroupAsync(string enrollmentGroupId, Attestation attestation)
    {
        EnrollmentGroup enrollmentGroup;
        try
        {
            enrollmentGroup = await _psc.GetEnrollmentGroupAsync(enrollmentGroupId).ConfigureAwait(false);
            Console.WriteLine($"Enrollment group {enrollmentGroupId} already exists. Updating...");
            enrollmentGroup.Attestation = attestation;
        }
        catch (ProvisioningServiceClientHttpException ex)
        {
            if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                Console.WriteLine($"Adding new enrollmentGroup {enrollmentGroupId}...");
                enrollmentGroup = new EnrollmentGroup(enrollmentGroupId, attestation);
            }
            else
            {
                throw;
            }
        }

        return enrollmentGroup;
    }
    private async Task ProvisionDevicesAsync(string enrollmentGroupId, int provisionDevicesCount, string primaryKey)
    {
        Console.WriteLine("Provisioning Devices...");
        ParallelOptions parallelOptions = new()
        {
            MaxDegreeOfParallelism = MAX_PARALLELISM
        };
        int provisioned = 0;
        Stopwatch watch = Stopwatch.StartNew();
        Timer timer = new Timer((state) =>
        {
            Console.WriteLine($"Devices provisioned: {provisioned}. Provision ratio: {provisioned / watch.Elapsed.TotalSeconds:0.00} reg/sec.");
        }, null, 5000, 5000);
        IEnumerable<int> deviceIndexes = Enumerable.Range(0, provisionDevicesCount);
        await Parallel.ForEachAsync(deviceIndexes, parallelOptions, async (i, token) =>
        {
            string deviceId = $"{enrollmentGroupId}-device-{i.ToString("0000")}";
            try
            {
                await ProvisionDeviceAsync(deviceId, primaryKey);
                Interlocked.Increment(ref provisioned);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error provisioning device {deviceId}. {ex}");
            }
        });
        timer.Change(Timeout.Infinite, Timeout.Infinite);
        Console.WriteLine($"Total Devices Provisioned: {provisioned}.");
        Console.WriteLine($"Total Seconds: {watch.Elapsed.TotalSeconds}.");
        Console.WriteLine($"Provision ratio: {provisioned / watch.Elapsed.TotalSeconds:0.00} reg/sec.");
    }


    private async Task ProvisionDeviceAsync(string deviceId, string primaryKey)
    {
        try
        {
            string key = ComputeDerivedSymmetricKey(primaryKey, deviceId);

            SecurityProvider symmetricKeyProvider = new SecurityProviderSymmetricKey(deviceId, key, null);
            ProvisioningTransportHandler mqttTransportHandler = new ProvisioningTransportHandlerMqtt(TransportFallbackType.TcpWithWebSocketFallback);

            ProvisioningDeviceClient pdc = ProvisioningDeviceClient.Create(_dpsEndpoint, _dpsScopeId, symmetricKeyProvider, mqttTransportHandler);

            await pdc.RegisterAsync();
        }
        catch (ProvisioningTransportException ex)
        {
            if (ex.IsTransient)
            {
                Console.WriteLine($"Transient error trying to provision {deviceId}. Message: {ex.Message} Delaying 1 minute to continue.");
                await Task.Delay(60000);
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Unexpected exception trying to provision {deviceId}. Exception details: {ex}");
            throw;
        }
    }

    private static string CreateSymetricKey(string masterKey, string keySeed)
    {

        using (var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(masterKey)))
        {
            return Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(keySeed)));
        }
    }
    private static string ComputeDerivedSymmetricKey(string masterKey, string deviceId)
    {
        using (var hmac = new HMACSHA256(Convert.FromBase64String(masterKey)))
        {
            return Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(deviceId)));
        }
    }

}
