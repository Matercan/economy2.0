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

    public async Task<bool> InitializeItem(bool oneTime, string itemName, long price, bool inInventory, int incomeSourceId = -1, string command = "")
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                await connection.OpenAsync();
                // Corrected SQL: You're missing @userId and @serverId parameters in the VALUES clause,
                // and 'price' and 'inInventory' should also be parameters.
                // Assuming 'inventories' table has server_id and user_id columns.
                // Assuming 'price' and 'in_inventory' columns exist and are handled by parameters.
                string insertSql = "INSERT INTO inventories (server_id, one_time, income_source_id, command, item_name, user_id, price, in_inventory) VALUES (@serverId, @oneTime, @incomeSourceId, @command, @itemName, @userId, @price, @inInventory)";

                using (MySqlCommand cmd = new MySqlCommand(insertSql, connection))
                {
                    // Always add all parameters that are specified in the SQL VALUES clause.
                    // If a value should be NULL in the DB, pass DBNull.Value.
                    cmd.Parameters.AddWithValue("@serverId", "1"); 
                    cmd.Parameters.AddWithValue("@oneTime", oneTime);
                    cmd.Parameters.AddWithValue("@itemName", itemName);
                    int userId = await GetDatabaseUserIdFromDiscordUserId("1", "1"); // Assuming "1" is the discord_user_id and "1" is the server_id for the user you want
                    if (userId == -1)
                    {
                        Console.WriteLine("Error: Could not find database user ID for the provided Discord user/server. Cannot initialize item.");
                        return false; // Or throw an exception
                    }
                    cmd.Parameters.AddWithValue("@userId", userId);
                    cmd.Parameters.AddWithValue("@price", price);
                    cmd.Parameters.AddWithValue("@inInventory", inInventory);

                    // Handle incomeSourceId: if it's -1 (your default), pass DBNull.Value
                    // Otherwise, pass the integer value.
                    if (incomeSourceId == -1)
                    {
                        cmd.Parameters.AddWithValue("@incomeSourceId", DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@incomeSourceId", incomeSourceId);
                    }

                    // Always add the @command parameter. If the C# 'command' string is empty,
                    // pass DBNull.Value to insert NULL into the database.
                    if (string.IsNullOrEmpty(command)) // Use string.IsNullOrEmpty for robustness
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
                Console.WriteLine($"Error initializing item: {ex.Message}"); // Changed error message for clarity
                Console.WriteLine(ex.ToString());
                return false;
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
        }
    }
}
