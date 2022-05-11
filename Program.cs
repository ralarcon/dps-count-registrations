using Microsoft.Azure.Devices.Provisioning.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((config) => { config.AddUserSecrets<Program>(); })
    .Build();

IConfiguration config = host.Services.GetRequiredService<IConfiguration>();

string dpsConnStr = config.GetValue<string>("Settings:Dps:ConnectionString");

using (ProvisioningServiceClient psc = ProvisioningServiceClient.CreateFromConnectionString(dpsConnStr))
{
    int enrollmentGroupsRegistrations = await GetEnrollmentGroupsRegistrationsCountAsync(psc).ConfigureAwait(false);

    int individualRegistrations = await GetIndividualRegistrationCountAsync(psc).ConfigureAwait(false);

    Console.WriteLine($"\nTotal: {enrollmentGroupsRegistrations + individualRegistrations} registrations.");
}

async Task<int> GetEnrollmentGroupsRegistrationsCountAsync(ProvisioningServiceClient psc)
{
    Console.WriteLine("Calculating device enrollment group registrations...");

    int totalEnrollmentGroupsRegistrations = 0;
    using (Query query = psc.CreateEnrollmentGroupQuery(new QuerySpecification("SELECT * FROM enrollmentGroups")))
    {
        while (query.HasNext())
        {
            QueryResult queryResult = await query.NextAsync().ConfigureAwait(false);
            
            ParallelOptions parallelOptions = new()
            {
                MaxDegreeOfParallelism = 15
            };

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
            new QuerySpecification("SELECT * FROM enrollmentGroups"), group.EnrollmentGroupId))
    {
        while (registrationsQuery.HasNext())
        {
            QueryResult registrationQueryResult = await registrationsQuery.NextAsync().ConfigureAwait(false);
            registrations += registrationQueryResult.Items.Count();
        }
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





