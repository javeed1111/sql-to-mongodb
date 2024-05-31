module.exports = {

    sqlConnectionString: "name="ATSTravel" connectionString="Data Source=104.211.182.161;Database=ATSPrelive3;Integrated Security=false;User ID=ganit;Password=GCVhvj534cfg;MultipleActiveResultSets=True" providerName="System.Data.SqlClient", // Insert your connection string here.
    mongoConnectionString: "mongodb://localhost:27017", // This puts the resulting database in MongoDB running on your local PC.
    targetDatabaseName: "target-database", // Specify the MongoDB database where the data will end up.
    skip: [
        "sql-table-to-skip-1", // Add the tables here that you don't want to replicate to MongoDB.
        "sql-table-to-skip-2"
    ],
    remapKeys: true // Set this to false if you want to leave table keys as they are, set to true to remap them to MongoDB ObjectId's.
};
