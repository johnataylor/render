using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;

namespace Render
{
    class Program
    {
        static JToken ToToken(SqlDataReader reader, int i)
        {
            if (reader.IsDBNull(i))
            {
                return JValue.Parse("null");
            }
            else if (typeof(bool).IsAssignableFrom(reader.GetFieldType(i)))
            {
                return new JValue((bool)reader.GetValue(i));
            }
            else if (typeof(int).IsAssignableFrom(reader.GetFieldType(i)))
            {
                return new JValue((int)reader.GetValue(i));
            }
            else if (typeof(double).IsAssignableFrom(reader.GetFieldType(i)))
            {
                return new JValue((double)reader.GetValue(i));
            }
            else
            {
                return new JValue(reader.GetValue(i).ToString());
            }
        }

        static JObject ToObject(SqlDataReader reader)
        {
            JObject item = new JObject();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                item.Add(reader.GetName(i), ToToken(reader, i));
            }
            return item;
        }

        static IDictionary<int, JObject> LoadDictionary(string connectionString, string cmdText)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(cmdText, connection);
                SqlDataReader reader = command.ExecuteReader();
                IDictionary<int, JObject> result = new Dictionary<int, JObject>();
                while (reader.Read())
                {
                    JObject row = ToObject(reader);
                    result.Add(row.Value<int>("Key"), row);
                }
                return result;
            }
        }

        static IList<JObject> LoadList(string connectionString, string cmdText)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(cmdText, connection);
                SqlDataReader reader = command.ExecuteReader();
                IList<JObject> result = new List<JObject>();
                while (reader.Read())
                {
                    JObject row = ToObject(reader);
                    result.Add(row);
                }
                return result;
            }
        }

        static int GetValue(string connectionString, string cmdText)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand(cmdText, connection);
                int val = (int)command.ExecuteScalar();
                return val;
            }
        }

        static IDictionary<int, IList<int>> GroupBy(IDictionary<int, JObject> relation, string fieldName)
        {
            IDictionary<int, IList<int>> result = new Dictionary<int, IList<int>>();
            foreach (KeyValuePair<int, JObject> tuple in relation)
            {
                int groupKey = tuple.Value.Value<int>(fieldName);
                IList<int> group;
                if (!result.TryGetValue(groupKey, out group))
                {
                    group = new List<int>();
                    result.Add(groupKey, group);
                }
                group.Add(tuple.Key);
            }
            return result;
        }

        static IDictionary<int, IList<int>> GroupBy(IList<JObject> relation, string groupFieldName, string dataFieldName)
        {
            IDictionary<int, IList<int>> result = new Dictionary<int, IList<int>>();
            foreach (JObject tuple in relation)
            {
                int groupKey = tuple.Value<int>(groupFieldName);
                IList<int> group;
                if (!result.TryGetValue(groupKey, out group))
                {
                    group = new List<int>();
                    result.Add(groupKey, group);
                }
                group.Add(tuple.Value<int>(dataFieldName));
            }
            return result;
        }

        static void SaveBlob(string storageConnectionString, string name, JToken content)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("nuget");
            CloudBlockBlob blob = container.GetBlockBlobReference(name);

            blob.Properties.ContentType = "application/json";
            blob.Properties.CacheControl = "no-cache";

            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            using (JsonWriter jsonWriter = new JsonTextWriter(writer))
            {
                jsonWriter.Formatting = Formatting.Indented;

                content.WriteTo(jsonWriter);
                jsonWriter.Flush();
                writer.Flush();
                stream.Seek(0, SeekOrigin.Begin);
                blob.UploadFromStream(stream);
            }

            //Console.WriteLine(name);
        }

        static JObject MakePackageDetailsContent(JObject package, IDictionary<int, JObject> packageDependencies, IDictionary<int, JObject> packageFrameworks, IDictionary<int, IList<int>> dependenciesByPackage, IDictionary<int, IList<int>> frameworksByPackage)
        {
            int packageKey = package.Value<int>("Key");

            package = (JObject)package.DeepClone();
            package.Remove("Key");
            package.Remove("PackageRegistrationKey");
            package.Remove("UserKey");

            IList<int> dependencyKeys;
            if (dependenciesByPackage.TryGetValue(packageKey, out dependencyKeys))
            {
                JArray dependencies = new JArray();
                foreach (int dependencyKey in dependencyKeys)
                {
                    JObject dependency = (JObject)packageDependencies[dependencyKey].DeepClone();
                    dependency.Remove("Key");
                    dependency.Remove("PackageKey");
                    dependency.Add("Uri", MakePackageRegistrationUri(dependency));
                    dependencies.Add(dependency);
                }
                package.Add("dependencies", dependencies);
            }

            IList<int> frameworkKeys;
            if (frameworksByPackage.TryGetValue(packageKey, out frameworkKeys))
            {
                JArray frameworks = new JArray();
                foreach (int frameworkKey in frameworkKeys)
                {
                    JObject framework = (JObject)packageFrameworks[frameworkKey].DeepClone();
                    framework.Remove("Key");
                    framework.Remove("Package_Key");
                    frameworks.Add(framework);
                }
                package.Add("frameworks", frameworks);
            }

            return package;
        }

        static JObject MakePackageContent(JObject package, IDictionary<int, JObject> packages, JObject packageRegistration, IList<int> packageKeys, IDictionary<int, JObject> packageDependencies, IDictionary<int, JObject> packageFrameworks, IDictionary<int, IList<int>> dependenciesByPackage, IDictionary<int, IList<int>> frameworksByPackage, IDictionary<int, JObject> users, IDictionary<int, IList<int>> ownersByRegistration)
        {
            JObject content = new JObject();
            content.Add("details", MakePackageDetailsContent(
                package, 
                packageDependencies, 
                packageFrameworks, 
                dependenciesByPackage, 
                frameworksByPackage));

            int packageRegistrationKey = packageRegistration.Value<int>("Key");

            IList<int> owners;
            if (ownersByRegistration.TryGetValue(packageRegistrationKey, out owners))
            {
                JArray ownersContent = new JArray();
                foreach (int userKey in owners)
                {
                    JObject obj;
                    if (!users.TryGetValue(userKey, out obj))
                    {
                        throw new Exception(string.Format("userKey {0} not found for packageRegistrationKey {1}", userKey, packageRegistrationKey));
                    }

                    JObject owner = (JObject)obj.DeepClone();
                    owner.Remove("Key");
                    owner.Add("Uri", MakeOwnerUri(owner));
                    ownersContent.Add(owner);
                }
                content.Add("owners", ownersContent);
            }
            else
            {
                //  package registration has no owners
            }

            JArray packagesContent = new JArray();
            foreach (int packageKey in packageKeys)
            {
                JObject obj;
                if (!packages.TryGetValue(packageKey, out obj))
                {
                    throw new Exception(string.Format("packageKey {0} not found in packages", packageKey));
                }

                JObject packageSummary = new JObject();
                packageSummary.Add("Version", obj["Version"]);
                packageSummary.Add("Listed", obj["Listed"]);
                packageSummary.Add("IsLatest", obj["IsLatest"]);
                packageSummary.Add("IsLatestStable", obj["IsLatestStable"]);
                packageSummary.Add("Downloads", obj["DownloadCount"]);
                packageSummary.Add("Published", obj["Published"]);
                packageSummary.Add("Uri", MakePackageUri(packageRegistration, obj));

                packagesContent.Add(packageSummary);
            }
            content.Add("packages", packagesContent);

            return content;
        }

        static string BaseUri = "http://linked.blob.core.windows.net/nuget/";

        static string MakePackageName(JObject packageRegistration, JObject package)
        {
            return string.Format("package/{0}/{1}", packageRegistration["Id"], package["Version"]).ToLowerInvariant();
        }

        static string MakePackageShortName(JObject packageRegistration)
        {
            return string.Format("package/{0}", packageRegistration["Id"]).ToLowerInvariant();
        }

        static string MakeOwnerName(JObject owner)
        {
            return string.Format("owner/{0}", owner["Username"]).ToLowerInvariant();
        }

        static string MakePackageUri(JObject packageRegistration, JObject package)
        {
            return BaseUri + MakePackageName(packageRegistration, package);
        }

        static string MakePackageRegistrationUri(JObject packageRegistration)
        {
            return BaseUri + MakePackageShortName(packageRegistration);
        }

        static string MakeOwnerUri(JObject owner)
        {
            return BaseUri + MakeOwnerName(owner);
        }

        static void CreatePackageBlobs(
            string storageConnectionString, 
            IDictionary<int, JObject> packageRegistrations, 
            IDictionary<int, JObject> packages,
            IDictionary<int, IList<int>> packagesByRegistration, 
            IDictionary<int, JObject> packageDependencies, 
            IDictionary<int, JObject> packageFrameworks, 
            IDictionary<int, IList<int>> dependenciesByPackage, 
            IDictionary<int, IList<int>> frameworksByPackage,
            IDictionary<int, JObject> users,
            IDictionary<int, IList<int>> ownersByRegistration)
        {
            foreach (JObject package in packages.Values)
            {
                JObject packageRegistration = packageRegistrations[package.Value<int>("PackageRegistrationKey")];

                JObject content = MakePackageContent(
                    package,
                    packages,
                    packageRegistration,
                    packagesByRegistration[packageRegistration.Value<int>("Key")],
                    packageDependencies,
                    packageFrameworks,
                    dependenciesByPackage,
                    frameworksByPackage,
                    users,
                    ownersByRegistration);

                SaveBlob(storageConnectionString, MakePackageName(packageRegistration, package), content);

                if (package.Value<bool>("IsLatest"))
                {
                    SaveBlob(storageConnectionString, MakePackageShortName(packageRegistration), content);
                }
            }
        }

        static void CreatePackageBlobs(string storageConnectionString, IDictionary<int, JObject> packageRegistrations, IDictionary<int, JObject> packages, IDictionary<int, JObject> packageDependencies, IDictionary<int, JObject> packageFrameworks, IDictionary<int, JObject> users, IDictionary<int, IList<int>> ownersByRegistration)
        {
            IDictionary<int, IList<int>> packagesByRegistration = GroupBy(packages, "PackageRegistrationKey");
            IDictionary<int, IList<int>> dependenciesByPackage = GroupBy(packageDependencies, "PackageKey");
            IDictionary<int, IList<int>> frameworksByPackage = GroupBy(packageFrameworks, "Package_Key");

            CreatePackageBlobs(
                storageConnectionString,
                packageRegistrations,
                packages,
                packagesByRegistration,
                packageDependencies,
                packageFrameworks,
                dependenciesByPackage,
                frameworksByPackage,
                users,
                ownersByRegistration);
        }

        static JObject MakeOwnerContent(JObject user, IDictionary<int, JObject> packageRegistrations, IList<int> registrationsByOwner)
        {
            JObject owner = (JObject)user.DeepClone();

            JArray registrations = new JArray();
            foreach (int packageRegistrationKey in registrationsByOwner)
            {
                JObject packageRegistration = packageRegistrations[packageRegistrationKey];

                JObject registration = new JObject();
                registration.Add("Id", packageRegistration["Id"]);
                registration.Add("Uri", MakePackageRegistrationUri(packageRegistration));
                registrations.Add(registration);
            }

            owner.Add("Packages", registrations);

            return owner;
        }

        static void CreateOwnerBlobs(string storageConnectionString, IDictionary<int, JObject> packageRegistrations, IDictionary<int, JObject> users, IDictionary<int, IList<int>> registrationsByOwner)
        {
            foreach (KeyValuePair<int, IList<int>> owner in registrationsByOwner)
            {
                JObject user = users[owner.Key];
                JObject content = MakeOwnerContent(user, packageRegistrations, owner.Value);
                string name = string.Format("owner/{0}", user["Username"]);
                SaveBlob(storageConnectionString, name, content);
            }
        }

        static Tuple<IDictionary<int, JObject>, IDictionary<int, IList<int>>, IDictionary<int, IList<int>>> FetchAllOwners(string connectionString)
        {
            IDictionary<int, JObject> users = LoadDictionary(connectionString, "SELECT [Key], [Username], [CreatedUtc] FROM Users");

            IList<JObject> packageRegistrationOwners = LoadList(connectionString, "SELECT * FROM PackageRegistrationOwners");
            IDictionary<int, IList<int>> registrationsByOwner = GroupBy(packageRegistrationOwners, "UserKey", "PackageRegistrationKey");
            IDictionary<int, IList<int>> ownersByRegistration = GroupBy(packageRegistrationOwners, "PackageRegistrationKey", "UserKey");

            return new Tuple<IDictionary<int, JObject>, IDictionary<int, IList<int>>, IDictionary<int, IList<int>>>(users, registrationsByOwner, ownersByRegistration);
        }

        static void ProcessOwners(string connectionString, string storageConnectionString, Tuple<IDictionary<int, JObject>, IDictionary<int, IList<int>>, IDictionary<int, IList<int>>> owners)
        {
            IDictionary<int, JObject> packageRegistrations = LoadDictionary(connectionString, "SELECT * FROM PackageRegistrations");

            CreateOwnerBlobs(
                storageConnectionString,
                packageRegistrations,
                owners.Item1,
                owners.Item2);
        }

        static void ProcessPackages(int? start, string connectionString, string storageConnectionString, Tuple<IDictionary<int, JObject>, IDictionary<int, IList<int>>, IDictionary<int, IList<int>>> owners)
        {
            int min = start ?? GetValue(connectionString, "SELECT MIN([Key]) FROM PackageRegistrations");
            int max = GetValue(connectionString, "SELECT MAX([Key]) FROM PackageRegistrations");

            const int chunk = 10;

            int prev = min;

            int accumPackages = 0;

            do
            {
                int low = prev;
                int high = low + chunk;

                prev = high;

                Console.WriteLine("low = {0}, high = {1}", low, high);

                string packagesCmdText = string.Format(
                    @"  SELECT Packages.* 
                        FROM Packages 
                        INNER JOIN PackageRegistrations ON Packages.[PackageRegistrationKey] = PackageRegistrations.[Key] 
                        WHERE PackageRegistrations.[Key] >= {0} AND PackageRegistrations.[Key] < {1}", low, high);

                IDictionary<int, JObject> packages = LoadDictionary(connectionString, packagesCmdText);

                accumPackages += packages.Count;

                string packageDependenciesCmdText = string.Format(
                    @"  SELECT PackageDependencies.* 
                        FROM PackageDependencies
                        INNER JOIN Packages ON Packages.[Key] = PackageDependencies.[PackageKey] 
                        INNER JOIN PackageRegistrations ON Packages.[PackageRegistrationKey] = PackageRegistrations.[Key] 
                        WHERE PackageRegistrations.[Key] >= {0} AND PackageRegistrations.[Key] < {1}", low, high);

                IDictionary<int, JObject> packageDependencies = LoadDictionary(connectionString, packageDependenciesCmdText);

                string packageFrameworksCmdText = string.Format(
                    @"  SELECT PackageFrameworks.* 
                        FROM PackageFrameworks
                        INNER JOIN Packages ON Packages.[Key] = PackageFrameworks.[Package_Key] 
                        INNER JOIN PackageRegistrations ON Packages.[PackageRegistrationKey] = PackageRegistrations.[Key] 
                        WHERE PackageRegistrations.[Key] >= {0} AND PackageRegistrations.[Key] < {1}", low, high);

                IDictionary<int, JObject> packageFrameworks = LoadDictionary(connectionString, packageFrameworksCmdText);

                string packageRegistrationsCmdText = string.Format(
                    @"  SELECT PackageRegistrations.* 
                        FROM PackageRegistrations 
                        WHERE PackageRegistrations.[Key] >= {0} AND PackageRegistrations.[Key] < {1}", low, high);

                IDictionary<int, JObject> packageRegistrations = LoadDictionary(connectionString, packageRegistrationsCmdText);

                Console.WriteLine("packages = {0}, accumulated = {1}", packages.Count, accumPackages);

                CreatePackageBlobs(storageConnectionString, packageRegistrations, packages, packageDependencies, packageFrameworks, owners.Item1, owners.Item3);
            }
            while (prev <= max);

            Console.WriteLine();
            Console.WriteLine("Total: {0}", accumPackages);
        }

        static void Main(string[] args)
        {
            string connectionString = ConfigurationManager.AppSettings["sql"];
            string storageConnectionString = ConfigurationManager.AppSettings["storage"];

            try
            {
                int? start = null;

                if (args.Length == 1)
                {
                    int result;
                    if (int.TryParse(args[0], out result))
                    {
                        start = result;
                    }
                }

                Tuple<IDictionary<int, JObject>, IDictionary<int, IList<int>>, IDictionary<int, IList<int>>> owners = FetchAllOwners(connectionString);
                ProcessPackages(start, connectionString, storageConnectionString, owners);
                ProcessOwners(connectionString, storageConnectionString, owners);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}", e.Message, e.StackTrace);
            }
        }
    }
}
