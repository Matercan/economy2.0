using MySql.Data.MySqlClient;


public class DatabaseManager
{
    private readonly string connectionString;

    public enum TransactionType
    {
       buy,
       sell,
       income,
       commands,
       daily,
       extra
    }
    
    public struct Item 
    {
        // Use properties (PascalCase)
        public int ItemId { get; set; }
        public string ItemName { get; set; }
        public long Price { get; set; }
        public bool OneTime { get; set; }
        public bool InInventory { get; set; }
        public string? Command { get; set; } 
        public int? IncomeSourceId { get; set; } 

        // Constructor to ensure all fields are initialized
        public Item(int itemId, string itemName, long price, bool oneTime, bool inInventory, string? command, int? incomeSourceId)
        {
            ItemId = itemId;
            ItemName = itemName;
            Price = price;
            OneTime = oneTime;
            InInventory = inInventory;
            Command = command;
            IncomeSourceId = incomeSourceId;
        }
    }
                

    public DatabaseManager()
    {
        // These are environment variables
        // Get environment variables, throwing an exception if they are not set
        string server = Environment.GetEnvironmentVariable("BOT_SERVER")
                        ?? throw new InvalidOperationException("Environment variable BOT_SERVER is not set.");
        string database = Environment.GetEnvironmentVariable("BOT_DATABASE")
                          ?? throw new InvalidOperationException("Environment variable BOT_DATABASE is not set.");
        string uid = Environment.GetEnvironmentVariable("BOT_USER_ID")
                     ?? throw new InvalidOperationException("Environment variable BOT_USER_ID is not set.");
        string password = Environment.GetEnvironmentVariable("BOT_PASSWORD")
                          ?? throw new InvalidOperationException("Environment variable BOT_PASSWORD is not set.");

        connectionString = $"SERVER={server};DATABASE={database};UID={uid};PASSWORD={password};";
    }

    public async Task TestConnectionAsync()
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try 
            {
                await connection.OpenAsync();
                Console.WriteLine("Succeessfully connected to MySQL database!");
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error connection to MySQL: {ex.Message}");
                Console.WriteLine(ex.ToString());
            }
        }
    }

    public async Task<bool> InsertUserAsync(string discordUserId, string serverId, string username)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                string insertSql = "INSERT INTO users (discord_user_id, server_id, username, cash_balance, bank_balance, last_daily_claim) VALUES (@discordUserId, @serverId, @username, @initialCash, @initialBank, @lastDailyClaim)";

                using (MySqlCommand cmd = new MySqlCommand(insertSql, connection))
                {
                    cmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                    cmd.Parameters.AddWithValue("@serverId", serverId);
                    cmd.Parameters.AddWithValue("@username", username);
                    cmd.Parameters.AddWithValue("@initialCash", 0); // Default starting cash
                    cmd.Parameters.AddWithValue("@initialBank", 0); // Default starting bank
                    cmd.Parameters.AddWithValue("@lastDailyClaim", DBNull.Value); // Cooldown starts as NULL

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    return rowsAffected > 0; // Should be 1 if successful
                }
            }
            catch (MySqlException ex)
            {
                // Handle specific MySQL errors, e.g., duplicate entry (error code 1062)
                if (ex.Number == 1062) // Duplicate entry error code
                {
                    Console.WriteLine($"User {username} (ID: {discordUserId}) already exists on server {serverId}. (Duplicate entry)");
                    return false; // Or re-throw if you want to handle it differently
                }
                Console.WriteLine($"Error inserting user: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return false;
            }
        }
    }

    public async Task<bool> InsertServerAsync(string discordServerId, string serverName)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                string insertSql = "INSERT INTO servers (server_id, server_name, economy_enabled, prefix) VALUES (@serverId, @serverName, @economyEnabled, @prefix)";

                using (MySqlCommand cmd = new MySqlCommand(insertSql, connection))
                {
                    cmd.Parameters.AddWithValue("@serverId", discordServerId);
                    cmd.Parameters.AddWithValue("@serverName", serverName);
                    // Corrected parameter name: economy_enabled
                    cmd.Parameters.AddWithValue("@economyEnabled", true); // Use the correct parameter name
                    cmd.Parameters.AddWithValue("@prefix", "m!");

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    return rowsAffected > 0;
                }
            }
            catch (MySqlException ex)
            {
                if (ex.Number == 1062)
                {
                    Console.WriteLine($"Server {serverName} ID: {discordServerId} already exists in the database. (Duplicate entry)");
                    return false;
                }
                Console.WriteLine($"Error inserting server: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return false;
            }
        }

    }
    
    public async Task<(long cash, long bank)> GetUserBalancesAsync(string discordUserId, string serverId)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                string sql = "SELECT cash_balance, bank_balance FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                    cmd.Parameters.AddWithValue("@serverId", serverId);

                    using (MySqlDataReader reader = (MySqlDataReader)await cmd.ExecuteReaderAsync())
                    {
                        if (reader.Read()) // If a row is found
                        {
                            long cash = reader.GetInt64("cash_balance");
                            long bank = reader.GetInt64("bank_balance");
                            return (cash, bank);
                        }
                        return (-1, -1); // User/server not found
                    }
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error retrieving user balances for {discordUserId} on {serverId}: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return (-1, -1); // Error occurred
            }
        }
    }

    public async Task<bool> AddOrUpdateUserAsync(string discordUserId, string serverId, string username)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            await connection.OpenAsync();
            MySqlTransaction? transaction = null;

            try
            {
               transaction = connection.BeginTransaction();

                string selectSql = "SELECT id FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                using (MySqlCommand selectCmd = new MySqlCommand(selectSql, connection, transaction))
                {
                    selectCmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                    selectCmd.Parameters.AddWithValue("@serverId", serverId);

                    using (MySqlDataReader reader = (MySqlDataReader)await selectCmd.ExecuteReaderAsync())
                    {
                        if (reader.Read())
                        {
                            // User exists, close reader and then perform an UPDATE
                            reader.Close();

                            string updateSql = "UPDATE users SET username = @username WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                            using (MySqlCommand updateCmd = new MySqlCommand(updateSql, connection, transaction))
                            {
                                updateCmd.Parameters.AddWithValue("@username", username);
                                updateCmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                                updateCmd.Parameters.AddWithValue("@serverId", serverId);
                                await updateCmd.ExecuteNonQueryAsync();
                                Console.WriteLine($"Updated username for {username} on server {serverId}.");
                            }
                        }
                        else
                        {
                            // User does not exist, close reader and then perform an INSERT
                            reader.Close();

                            string insertSql = "INSERT INTO users (discord_user_id, server_id, username, cash_balance, bank_balance, last_daily_claim) VALUES (@discordUserId, @serverId, @username, @initialCash, @initialBank, @lastDailyClaim)";
                            using (MySqlCommand insertCmd = new MySqlCommand(insertSql, connection, transaction))
                            {
                                insertCmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                                insertCmd.Parameters.AddWithValue("@serverId", serverId);
                                insertCmd.Parameters.AddWithValue("@username", username);
                                insertCmd.Parameters.AddWithValue("@initialCash", 0);
                                insertCmd.Parameters.AddWithValue("@initialBank", 0);
                                insertCmd.Parameters.AddWithValue("@lastDailyClaim", DBNull.Value);
                                await insertCmd.ExecuteNonQueryAsync();
                                Console.WriteLine($"Added new user {username} (ID: {discordUserId}) to server {serverId}.");
                            }
                        }
                    }
                }
                transaction.Commit();
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error adding/updating user: {ex.Message}");
                Console.WriteLine(ex.ToString());
                transaction?.Rollback();
                return false;
            }
        }
    }

    public async Task<int> GetDatabaseUserIdFromDiscordUserId(string discordUserId, string serverId)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                string sql = "SELECT id FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                    cmd.Parameters.AddWithValue("@serverId", serverId);

                    object result = await cmd.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToInt32(result);
                    }
                    return -1; // Or throw an exception if user is expected to exist
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error retrieving database user ID for {discordUserId} on {serverId}: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return -1; // Indicate failure
            }
        }
    }

    public async Task<bool> AddCashToUserAsync(string discordUserId, string serverId, long amount, TransactionType cause, string description)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            await connection.OpenAsync();
            MySqlTransaction? transaction = null; // Declare transaction here

            try
            {
                transaction = connection.BeginTransaction(); // Start the transaction

                // 1. Get the internal database user ID
                int userId = await GetDatabaseUserIdFromDiscordUserId(discordUserId, serverId);
                if (userId == -1)
                {
                    Console.WriteLine($"User {discordUserId} not found on server {serverId}. Cannot add cash or record transaction.");
                    transaction.Rollback(); // Rollback if user not found
                    return false;
                }

                // 2. Update the user's cash balance
                string updateBalanceSql = "UPDATE users SET cash_balance = cash_balance + @amount WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                using (MySqlCommand updateCmd = new MySqlCommand(updateBalanceSql, connection, transaction)) // Pass transaction
                {
                    updateCmd.Parameters.AddWithValue("@amount", amount);
                    updateCmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                    updateCmd.Parameters.AddWithValue("@serverId", serverId);
                    int rowsAffected = await updateCmd.ExecuteNonQueryAsync();
                    if (rowsAffected == 0)
                    {
                        Console.WriteLine($"Failed to update cash balance for user {discordUserId} on server {serverId}. User might not exist.");
                        transaction.Rollback(); // Rollback if update failed (e.g., user not found)
                        return false;
                    }
                }

                // 3. Insert the transaction record
                string insertTransactionSql = "INSERT INTO transactions (user_id, server_id, type, amount, timestamp, description) VALUES (@userId, @serverId, @transactionType, @amount, @timestamp, @description)";
                using (MySqlCommand insertCmd = new MySqlCommand(insertTransactionSql, connection, transaction)) // Pass transaction
                {
                    insertCmd.Parameters.AddWithValue("@userId", userId);
                    insertCmd.Parameters.AddWithValue("@serverId", serverId);
                    insertCmd.Parameters.AddWithValue("@transactionType", cause.ToString().ToLower()); // Convert enum to lowercase string
                    insertCmd.Parameters.AddWithValue("@amount", amount);
                    insertCmd.Parameters.AddWithValue("@timestamp", DateTime.UtcNow);
                    insertCmd.Parameters.AddWithValue("@description", description);
                    await insertCmd.ExecuteNonQueryAsync();
                }

                transaction.Commit(); // Commit the transaction if all operations succeeded
                Console.WriteLine($"Successfully added {amount} cash to {discordUserId} on {serverId} and recorded transaction.");
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error adding cash and recording transaction for {discordUserId} on {serverId}: {ex.Message}");
                Console.WriteLine(ex.ToString());
                transaction?.Rollback(); // Rollback on any error
                return false;
            }
        }
    }

     public async Task<bool> AddBankToUserAsync(string discordUserId, string serverId, long amount, TransactionType cause, string description)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            await connection.OpenAsync();
            MySqlTransaction? transaction = null; // Declare transaction here

            try
            {
                transaction = connection.BeginTransaction(); // Start the transaction

                // 1. Get the internal database user ID
                int userId = await GetDatabaseUserIdFromDiscordUserId(discordUserId, serverId);
                if (userId == -1)
                {
                    Console.WriteLine($"User {discordUserId} not found on server {serverId}. Cannot add cash or record transaction.");
                    transaction.Rollback(); // Rollback if user not found
                    return false;
                }

                // 2. Update the user's cash balance
                string updateBalanceSql = "UPDATE users SET bank_balance = bank_balance + @amount WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                using (MySqlCommand updateCmd = new MySqlCommand(updateBalanceSql, connection, transaction)) // Pass transaction
                {
                    updateCmd.Parameters.AddWithValue("@amount", amount);
                    updateCmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                    updateCmd.Parameters.AddWithValue("@serverId", serverId);
                    int rowsAffected = await updateCmd.ExecuteNonQueryAsync();
                    if (rowsAffected == 0)
                    {
                        Console.WriteLine($"Failed to update cash balance for user {discordUserId} on server {serverId}. User might not exist.");
                        transaction.Rollback(); // Rollback if update failed (e.g., user not found)
                        return false;
                    }
                }

                // 3. Insert the transaction record
                string insertTransactionSql = "INSERT INTO transactions (user_id, server_id, type, amount, timestamp, description) VALUES (@userId, @serverId, @transactionType, @amount, @timestamp, @description)";
                using (MySqlCommand insertCmd = new MySqlCommand(insertTransactionSql, connection, transaction)) // Pass transaction
                {
                    insertCmd.Parameters.AddWithValue("@userId", userId);
                    insertCmd.Parameters.AddWithValue("@serverId", serverId);
                    insertCmd.Parameters.AddWithValue("@transactionType", cause.ToString().ToLower()); // Convert enum to lowercase string
                    insertCmd.Parameters.AddWithValue("@amount", amount);
                    insertCmd.Parameters.AddWithValue("@timestamp", DateTime.UtcNow);
                    insertCmd.Parameters.AddWithValue("@description", description);
                    await insertCmd.ExecuteNonQueryAsync();
                }

                transaction.Commit(); // Commit the transaction if all operations succeeded
                Console.WriteLine($"Successfully added {amount} bank to {discordUserId} on {serverId} and recorded transaction.");
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error adding cash and recording transaction for {discordUserId} on {serverId}: {ex.Message}");
                Console.WriteLine(ex.ToString());
                transaction?.Rollback(); // Rollback on any error
                return false;
            }
        }

    }

    public async Task<bool> InitializeItem(
        bool oneTime,
        string itemName,
        long price,
        bool inInventory,
        int incomeSourceId = -1,
        string command = "")
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();

                // CORRECTED SQL: Fixed typo in 'in_inventory' column name.
                // Also, ensure 'item_id' is NOT in the column list if it's AUTO_INCREMENT.
                string insertSql = "INSERT INTO master_items (one_time, income_source_id, command, item_name, price, in_inventory) " +
                                   "VALUES (@oneTime, @incomeSourceId, @command, @itemName, @price, @inInventory)";

                using (MySqlCommand cmd = new MySqlCommand(insertSql, connection))
                {
                    // Add all parameters corresponding to the SQL statement
                    cmd.Parameters.AddWithValue("@itemName", itemName);
                    cmd.Parameters.AddWithValue("@price", price);
                    cmd.Parameters.AddWithValue("@inInventory", inInventory);
                    cmd.Parameters.AddWithValue("@oneTime", oneTime);

                    // Handle nullable incomeSourceId: if it's -1 (your default for null), pass DBNull.Value
                    if (incomeSourceId == -1)
                    {
                        cmd.Parameters.AddWithValue("@incomeSourceId", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@incomeSourceId", incomeSourceId);
                    }

                    // Handle nullable command: if the C# 'command' string is null or empty, pass DBNull.Value
                    if (string.IsNullOrEmpty(command))
                    {
                        cmd.Parameters.AddWithValue("@command", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@command", command);
                    }

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    return rowsAffected > 0;
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error initializing item: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return false;
            }
        }
    }


    public async Task<bool> AddItemToUser(int userId, int masterItemId, string serverId) // Renamed for clarity
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();

                // 1. Fetch the master item details
                //
                
                if (masterItemId == -1)
                {
                    Console.WriteLine($"Error: Item '{masterItemId}' not found in master list. Cannot add to inventory.");
                    return false;
                }

                Item itemDetails = await ReadItem(masterItemId);
                if (itemDetails.ItemId == 0)
                {
                    Console.WriteLine($"Error: Master Item with ID {masterItemId} not found. Cannot add to inventory.");
                    return false;
                }

                // 2. Prepare the INSERT statement for the 'inventories' table
                string insertSql = "INSERT INTO inventories (server_id, one_time, income_source_id, command, item_name, user_id, price, in_inventory, master_item_id) " +
                                   "VALUES (@serverId, @oneTime, @incomeSourceId, @command, @itemName, @userId, @price, @inInventory, @masterItemId)";

                using (MySqlCommand cmd = new MySqlCommand(insertSql, connection))
                {
                    // Parameters for the SQL query
                    cmd.Parameters.AddWithValue("@serverId", serverId);
                    cmd.Parameters.AddWithValue("@userId", userId);
                    cmd.Parameters.AddWithValue("@masterItemId", masterItemId); // The ID from your master item list

                    // Use properties from the fetched itemDetails object (PascalCase)
                    cmd.Parameters.AddWithValue("@oneTime", itemDetails.OneTime);
                    cmd.Parameters.AddWithValue("@itemName", itemDetails.ItemName);
                    cmd.Parameters.AddWithValue("@price", itemDetails.Price);
                    cmd.Parameters.AddWithValue("@inInventory", itemDetails.InInventory); // Consider if this should always be true for a newly added item

                    // Handle nullable IncomeSourceId
                    cmd.Parameters.AddWithValue("@incomeSourceId", itemDetails.IncomeSourceId.HasValue ? itemDetails.IncomeSourceId.Value : DBNull.Value);

                    // Handle nullable Command
                    cmd.Parameters.AddWithValue("@command", string.IsNullOrEmpty(itemDetails.Command) ? DBNull.Value : (object)itemDetails.Command);

                    int rowsAffected = await cmd.ExecuteNonQueryAsync();
                    return rowsAffected > 0;
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error when adding item {masterItemId} to user {userId} of {serverId}'s inventory: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return false;
            }
        }
    }

    public async Task<Item> ReadItem(int itemId) // Return Item? to indicate it might be null
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                // CORRECTED SQL: Added 'item_id' to the SELECT list
                string sql = "SELECT item_id, price, one_time, in_inventory, item_name, income_source_id, command FROM master_items WHERE item_id = @itemId";
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@itemId", itemId);

                    using (MySqlDataReader reader = (MySqlDataReader)await cmd.ExecuteReaderAsync())
                    {
                        if (reader.Read())
                        {
                            // Safely read nullable columns
                            string? command;
                            int commandDatabaseIndex = 6; // 7 is the index of command on the database, so we need to subtract 1 from it
                            if ( reader.IsDBNull(commandDatabaseIndex) ) { command = String.Empty; }
                            else { command = reader.GetString("command"); }

                            int? incomeSourceId;
                            int incomeSourceIdDatabseIndex = 5; // 6 is the index of income_source_id in the database, so we need to subtract 1 from it
                            if ( reader.IsDBNull(incomeSourceIdDatabseIndex) ) { incomeSourceId = null; }
                            else { incomeSourceId = reader.GetInt32("income_source_Id"); }

                            // Use the constructor to properly initialize the struct
                            return new Item(
                                itemId: reader.GetInt32("item_id"), // Now safe to read
                                itemName: reader.GetString("item_name"),
                                price: reader.GetInt64("price"),
                                oneTime: reader.GetBoolean("one_time"),
                                inInventory: reader.GetBoolean("in_inventory"),
                                command: command,
                                incomeSourceId: incomeSourceId
                            );
                        }
                        else
                        {
                            Console.WriteLine($"Item with ID {itemId} not found in master_items.");
                            return new Item();
                        }
                    }
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error reading item {itemId} from master_items: {ex.Message}");
                Console.WriteLine(ex.ToString());
                return new Item();
            }
        }
    }

    public async Task<bool> ClearDatabaseForTestingAsync(bool resetAutoIncrement = true)
    {
        // Define the order of tables to clear based on foreign key dependencies
        // Child tables first, then parent tables.
        List<string> tablesToClear = new List<string>
        {
            "inventories",   // References users, master_items
            "transactions",  // References users, servers
            "users",         // References servers
            "master_items",  // No outgoing FKs to these tables
            "servers",        // No outgoing FKs to these tables
            "incomes"
        };

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            await connection.OpenAsync();
            MySqlTransaction transaction = null;

            try
            {
                transaction = connection.BeginTransaction();
                Console.WriteLine("\n--- Clearing Database for Testing ---");

                foreach (string tableName in tablesToClear)
                {
                    string deleteSql = $"DELETE FROM `{tableName}`"; // Use backticks for table names
                    Console.WriteLine($"Deleting from table: {tableName}...");
                    using (MySqlCommand cmd = new MySqlCommand(deleteSql, connection, transaction))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }

                    if (resetAutoIncrement)
                    {
                        // Reset AUTO_INCREMENT for tables that have one
                        // This is a separate command, often executed after DELETE
                        string resetAutoIncrementSql = $"ALTER TABLE `{tableName}` AUTO_INCREMENT = 1";
                        Console.WriteLine($"Resetting AUTO_INCREMENT for {tableName}...");
                        using (MySqlCommand cmd = new MySqlCommand(resetAutoIncrementSql, connection, transaction))
                        {
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }

                transaction.Commit();
                Console.WriteLine("Database cleared successfully for testing!");
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error clearing database: {ex.Message}");
                Console.WriteLine(ex.ToString());
                transaction?.Rollback(); // Rollback all changes on error
                return false;
            }
        }
    }

    public async Task<int> GetMasterIndexFromItemName(string itemName)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                // CORRECTED: Select item_id from your master items table (e.g., 'items_master')
                string sql = "SELECT item_id FROM master_items WHERE item_name = @itemName";

                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@itemName", itemName);
                    // Using ExecuteScalarAsync is more efficient for single value retrieval
                    object result = await cmd.ExecuteScalarAsync();

                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToInt32(result); // CORRECTED: Convert the result of master_item_id
                    }
                    else
                    {
                        return -1; // Item not found
                    }
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error retrieving master item ID for '{itemName}': {ex.Message}");
                Console.WriteLine(ex.ToString());
                return -1;
            }
        }
    }

}

namespace economyBot
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            // Start the database
            Console.WriteLine("Starting economyBot...");
            DatabaseManager dbManager = new DatabaseManager();
            await dbManager.ClearDatabaseForTestingAsync();
            Console.WriteLine("------ TESTING BEGIN ------");

            // Ensure global economy and user are set up
            await dbManager.InsertServerAsync("1", "Global Economy");
            await dbManager.InsertUserAsync("1", "1", "Global User");

            // Start the user and server
            await dbManager.TestConnectionAsync();
            await dbManager.InsertServerAsync("1234", "The squad");
            await dbManager.InsertUserAsync("1234", "1234", "Matercan");

            // Add in money
            Console.WriteLine($"User 1234 balance: {await dbManager.GetUserBalancesAsync("1234", "1234")}");
            await dbManager.AddCashToUserAsync("1234", "1234", 100, DatabaseManager.TransactionType.income, "test");
            await dbManager.AddBankToUserAsync("1234", "1234", 1000, DatabaseManager.TransactionType.sell, "test");
            Console.WriteLine($"User 1234 balance: {await dbManager.GetUserBalancesAsync("1234", "1234")}");

            // Items
            await dbManager.InitializeItem(true, "test", 500, false);
            int itemIndex = await dbManager.GetMasterIndexFromItemName("test");
            await dbManager.AddItemToUser(await dbManager.GetDatabaseUserIdFromDiscordUserId("1234", "1234"), itemIndex, "1234");
        }
    }
}
