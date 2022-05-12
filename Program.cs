using Microsoft.Azure.Devices.Provisioning.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using dps_count_records;
using System.Diagnostics;

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((config) => { config.AddUserSecrets<Program>(); })
    .Build();

IConfiguration config = host.Services.GetRequiredService<IConfiguration>();

string dpsConnStr = config.GetValue<string>("Settings:Dps:ConnectionString");

using (ProvisioningServiceClient psc = ProvisioningServiceClient.CreateFromConnectionString(dpsConnStr))
{
    Stopwatch watch = Stopwatch.StartNew();
    int enrollmentGroupsRegistrations = await GetEnrollmentGroupsRegistrationsCountAsync(psc).ConfigureAwait(false);

    int individualRegistrations = await GetIndividualRegistrationCountAsync(psc).ConfigureAwait(false);

    Console.WriteLine($"\nTotal: {enrollmentGroupsRegistrations + individualRegistrations} registrations. [Duration: {watch.Elapsed.TotalSeconds:0.##} seconds])");
}

async Task<int> GetEnrollmentGroupsRegistrationsCountAsync(ProvisioningServiceClient psc)
{
    Console.WriteLine("Calculating device enrollment group registrations...");

    int totalEnrollmentGroupsRegistrations = 0;
    using (Query query = psc.CreateEnrollmentGroupQuery(new QuerySpecification("SELECT * FROM enrollmentGroups"), 1024))
    {
        ParallelOptions parallelOptions = new()
        {
            MaxDegreeOfParallelism = 15
        };

        while (query.HasNext())
        {
            QueryResult queryResult = await query.NextAsync().ConfigureAwait(false);
            
            await Parallel.ForEachAsync(queryResult.Items.Select(i => i as EnrollmentGroup), parallelOptions, async (group, token) =>
            {
                if (group != null)
                {
                    int regCount = await GetRegistrationsCountAsync(psc, group).ConfigureAwait(false);
                    Console.WriteLine($"Enrollment Group {group.EnrollmentGroupId}: {regCount} registrations.");
                    totalEnrollmentGroupsRegistrations += regCount;
                }
            });
        }
    }
    Console.WriteLine($"Group registrations: {totalEnrollmentGroupsRegistrations} registrations.");
    return totalEnrollmentGroupsRegistrations;
}

async Task<int> GetRegistrationsCountAsync(ProvisioningServiceClient psc, EnrollmentGroup group)
{
    int registrations = 0;
    using (Query registrationsQuery = psc.CreateEnrollmentGroupRegistrationStateQuery(
            new QuerySpecification("SELECT * FROM enrollmentGroups"), group.EnrollmentGroupId, 5000))
    {
        Timer timer = new Timer((state) =>
        {
            Console.WriteLine($"Calculating registrations in {group.EnrollmentGroupId}: {registrations} so far...");
        }, null, 5000, 5000);


        while (registrationsQuery.HasNext())
        {
            try
            {
                QueryResult registrationQueryResult = await registrationsQuery.NextAsync().ConfigureAwait(false);
                registrations += registrationQueryResult.Items.Count();
            }
            catch (ProvisioningServiceClientHttpException ex) when (ex.IsTransient && ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                Console.WriteLine($"\tTransient error retriving registrations page. {ex.ErrorMessage}.");
                if (ex.Fields.Keys.Contains("Retry-After") && int.TryParse(ex.Fields["Retry-After"], out var delaySeconds))
                {
                    Console.WriteLine($"\tOperation will retry after {delaySeconds} seconds.");
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                }
            }
        }
        timer.Change(Timeout.Infinite, Timeout.Infinite);
    }
    return registrations;
}

async Task<int> GetIndividualRegistrationCountAsync(ProvisioningServiceClient psc)
{
    Console.WriteLine("\nCalculating individual registrations...");
    int individualRegistrations = 0;
    using (Query query = psc.CreateIndividualEnrollmentQuery(new QuerySpecification("SELECT * FROM enrollments")))
    {
        while (query.HasNext())
        {
            QueryResult queryResult = await query.NextAsync().ConfigureAwait(false);
            individualRegistrations += queryResult.Items.Count();
        }
        Console.WriteLine($"Individual Registrations: {individualRegistrations} registrations.");
    }
    return individualRegistrations;
}





