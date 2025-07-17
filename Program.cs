using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics; // For Stopwatch


public class DatabaseManager : IDisposable
{
    private readonly string connectionString;
    private MySqlConnection _serverConnection;

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

    public struct Income
    {
        public int IncomeId { get; set; }
        public string IncomeName { get; set; }
        public long Amount { get; set; }
        public bool Percent { get; set; }
        public TimeSpan Cooldown { get; set; }
        public int? ItemSourceId { get; set; }

        public Income(int incomeId, string incomeName, long amount, bool percent, TimeSpan cooldown, int? itemSourceId)
        {
            IncomeId = incomeId;
            IncomeName = incomeName;
            Amount = amount;
            Percent = percent;
            Cooldown = cooldown;
            ItemSourceId = itemSourceId;
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
        _serverConnection = new MySqlConnection(connectionString);
    }

    /// <summary>
    /// Opens the database connection if it's not already open.
    /// This should be called once at the start of your application's database interactions.
    /// </summary>
    public async Task ConnectAsync()
    {
        if (_serverConnection.State == ConnectionState.Closed || _serverConnection.State == ConnectionState.Broken)
        {
            try
            {
                await _serverConnection.OpenAsync(); // <--- Use OpenAsync
                Console.WriteLine("Successfully connected to MySQL database!");
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"Error connecting to MySQL: {ex.Message}");
                Console.WriteLine(ex.ToString());
                throw; // Re-throw to indicate a critical connection failure
            }
        }
        else if (_serverConnection.State == ConnectionState.Open)
        {
            Console.WriteLine("Already connected to MySQL database!");
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (_serverConnection != null)
            {
                if (_serverConnection.State != ConnectionState.Closed)
                {
                    _serverConnection.Close();
                }
                _serverConnection.Dispose();
                _serverConnection = null;
            }
        }
    }

    // --- All other methods now use the _serverConnection field directly ---
    private void EnsureConnectionOpen()
    {
        if (_serverConnection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("Database connection is not open. Call ConnectAsync first and ensure it succeeds.");
        }
    }


    public async Task TestConnectionAsync()
    {
        EnsureConnectionOpen(); // Check if connection is open
        Console.WriteLine("Connection is active and ready.");
    }

    public async Task<bool> InsertUserAsync(string discordUserId, string serverId, string username)
    {
        EnsureConnectionOpen(); // Check if connection is open
        MySqlTransaction? transaction = null; // Declare transaction here

        try
        {
            // If this method is part of a larger transaction, it should accept it as a parameter.
            // For simplicity, I'm assuming it starts its own if not part of a larger one.
            // If you intend for AddOrUpdateUserAsync to handle its own transaction,
            // then it should begin and commit/rollback its own transaction.
            // If it's always called within an existing transaction, remove this.
            transaction = _serverConnection.BeginTransaction(); // Start transaction here

            string selectSql = "SELECT id FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
            using (MySqlCommand selectCmd = new MySqlCommand(selectSql, _serverConnection, transaction)) // Pass transaction
            {
                selectCmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                selectCmd.Parameters.AddWithValue("@serverId", serverId);

                using (MySqlDataReader reader = (MySqlDataReader)await selectCmd.ExecuteReaderAsync())
                {
                    if (reader.Read())
                    {
                        reader.Close(); // Close reader before new command

                        string updateSql = "UPDATE users SET username = @username WHERE discord_user_id = @discordUserId AND server_id = @serverId";
                        using (MySqlCommand updateCmd = new MySqlCommand(updateSql, _serverConnection, transaction)) // Pass transaction
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
                        reader.Close(); // Close reader before new command

                        string insertSql = "INSERT INTO users (discord_user_id, server_id, username, cash_balance, bank_balance, last_daily_claim) VALUES (@discordUserId, @serverId, @username, @initialCash, @initialBank, @lastDailyClaim)";
                        using (MySqlCommand insertCmd = new MySqlCommand(insertSql, _serverConnection, transaction)) // Pass transaction
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
            transaction.Commit(); // Commit transaction here
            return true;
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error adding/updating user: {ex.Message}");
            Console.WriteLine(ex.ToString());
            transaction?.Rollback(); // Rollback on error
            return false;
        }
    }

    public async Task<bool> InsertServerAsync(string discordServerId, string serverName)
    {
        EnsureConnectionOpen(); // Check if connection is open
        try
        {
            string insertSql = "INSERT INTO servers (server_id, server_name, economy_enabled, prefix) VALUES (@serverId, @serverName, @economyEnabled, @prefix)";
            using (MySqlCommand cmd = new MySqlCommand(insertSql, _serverConnection))
            {
                cmd.Parameters.AddWithValue("@serverId", discordServerId);
                cmd.Parameters.AddWithValue("@serverName", serverName);
                cmd.Parameters.AddWithValue("@economyEnabled", true);
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

    public async Task<(long cash, long bank)> GetUserBalancesAsync(string discordUserId, string serverId)
    {
        EnsureConnectionOpen(); // Check if connection is open
        try
        {
            string sql = "SELECT cash_balance, bank_balance FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
            using (MySqlCommand cmd = new MySqlCommand(sql, _serverConnection))
            {
                cmd.Parameters.AddWithValue("@discordUserId", discordUserId);
                cmd.Parameters.AddWithValue("@serverId", serverId);

                using (MySqlDataReader reader = (MySqlDataReader)await cmd.ExecuteReaderAsync())
                {
                    if (reader.Read())
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
    public async Task<bool> AddOrUpdateUserAsync(string discordUserId, string serverId, string username)
    {
        
        
        MySqlTransaction? transaction = null;

        try
        {
           transaction = _serverConnection.BeginTransaction();

            string selectSql = "SELECT id FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
            using (MySqlCommand selectCmd = new MySqlCommand(selectSql, _serverConnection, transaction))
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
                        using (MySqlCommand updateCmd = new MySqlCommand(updateSql, _serverConnection, transaction))
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
                        using (MySqlCommand insertCmd = new MySqlCommand(insertSql, _serverConnection, transaction))
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

    public async Task<int> GetDatabaseUserIdFromDiscordUserId(string discordUserId, string serverId)
    {
        EnsureConnectionOpen(); 
        try
        {
            string sql = "SELECT id FROM users WHERE discord_user_id = @discordUserId AND server_id = @serverId";
            using (MySqlCommand cmd = new MySqlCommand(sql, _serverConnection))
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

    public async Task<bool> AddCashToUserAsync(string discordUserId, string serverId, long amount, TransactionType cause, string description)
    {
        
        EnsureConnectionOpen(); 
        MySqlTransaction? transaction = null; // Declare transaction here

        try
        {
            transaction = _serverConnection.BeginTransaction(); // Start the transaction

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
            using (MySqlCommand updateCmd = new MySqlCommand(updateBalanceSql, _serverConnection, transaction)) // Pass transaction
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
            using (MySqlCommand insertCmd = new MySqlCommand(insertTransactionSql, _serverConnection, transaction)) // Pass transaction
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

     public async Task<bool> AddBankToUserAsync(string discordUserId, string serverId, long amount, TransactionType cause, string description)
    {
        {
            
            EnsureConnectionOpen(); 
            MySqlTransaction? transaction = null; // Declare transaction here

            try
            {
                transaction = _serverConnection.BeginTransaction(); // Start the transaction

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
                using (MySqlCommand updateCmd = new MySqlCommand(updateBalanceSql, _serverConnection, transaction)) // Pass transaction
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
                using (MySqlCommand insertCmd = new MySqlCommand(insertTransactionSql, _serverConnection, transaction)) // Pass transaction
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
        EnsureConnectionOpen(); 
        try
        {
            string insertSql = "INSERT INTO master_items (one_time, income_source_id, command, item_name, price, in_inventory) " +
                               "VALUES (@oneTime, @incomeSourceId, @command, @itemName, @price, @inInventory)";

            using (MySqlCommand cmd = new MySqlCommand(insertSql, _serverConnection))
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


    public async Task<bool> AddItemToUser(int userId, int masterItemId, string serverId) // Renamed for clarity
    {
        EnsureConnectionOpen(); 
        try
        {
            // 1. Fetch the master item details
            
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
            
            // Add in income if there is one
            if (itemDetails.IncomeSourceId.HasValue)
            {
                await AddIncomeToUser(userId, itemDetails.IncomeSourceId.Value, serverId);
            }

            // 2. Prepare the INSERT statement for the 'inventories' table
            string insertSql = "INSERT INTO inventories (server_id, one_time, income_source_id, command, item_name, user_id, price, in_inventory, master_item_id) " +
                               "VALUES (@serverId, @oneTime, @incomeSourceId, @command, @itemName, @userId, @price, @inInventory, @masterItemId)";

            using (MySqlCommand cmd = new MySqlCommand(insertSql, _serverConnection))
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
                cmd.Parameters.AddWithValue("@incomeSourceId", itemDetails.IncomeSourceId.HasValue ? itemDetails.IncomeSourceId.Value : null);

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

    public async Task<Item> ReadItem(int itemId) // Return Item? to indicate it might be null
    {
        EnsureConnectionOpen(); 
        try
        {
            // SQL: Select all necessary columns, including item_id
            string sql = "SELECT item_id, price, one_time, in_inventory, item_name, income_source_id, command FROM master_items WHERE item_id = @itemId";
            using (MySqlCommand cmd = new MySqlCommand(sql, _serverConnection))
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
                        return new Item(); // Return null if item not found
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error reading item {itemId} from master_items: {ex.Message}");
            Console.WriteLine(ex.ToString());
            return new Item(); // Return null on error
        }
        
    }

    

    public async Task<int> GetMasterIndexFromItemName(string itemName)
    {
        EnsureConnectionOpen(); 
        try
        {
            string sql = "SELECT item_id FROM master_items WHERE item_name = @itemName";

            using (MySqlCommand cmd = new MySqlCommand(sql, _serverConnection))
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
    
    public async Task<bool> InitializeIncome(
        string incomeName,
        long amount,
        bool percent,
        TimeSpan cooldown, // Keep TimeSpan for C# logic
        int itemSourceId = -1
    )
    {
        EnsureConnectionOpen(); 
        try
        {
            string inserSql = "INSERT INTO master_incomes (cooldown, income_name, percent, amount, item_source_id) " +
                              "VALUES (@cooldownSeconds, @incomeName, @percent, @amount, @itemSourceId)"; // Use @cooldownSeconds

            using (MySqlCommand cmd = new MySqlCommand(inserSql, _serverConnection))
            {
                cmd.Parameters.AddWithValue("@incomeName", incomeName);
                cmd.Parameters.AddWithValue("@amount", amount);
                cmd.Parameters.AddWithValue("@percent", percent);
                cmd.Parameters.AddWithValue("@cooldownSeconds", (long)cooldown.TotalSeconds); // <--- Convert TimeSpan to total seconds (long)

                if (itemSourceId == -1) { cmd.Parameters.AddWithValue("@itemSourceId", DBNull.Value); }
                else { cmd.Parameters.AddWithValue("@itemSourceId", itemSourceId); }

                int rowsAffected = await cmd.ExecuteNonQueryAsync();
                return rowsAffected > 0;
            }
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error initializing income: {ex.Message}");
            Console.WriteLine(ex.ToString());
            return false;
        }
        
    }


    public async Task<bool> AddIncomeToUser(int userId, int masterIncomeId, string serverId)
    {
        EnsureConnectionOpen(); 
        try
        {
            // 1. Fetch the master income details
            
            if (masterIncomeId == -1)
            {
                Console.WriteLine($"Error: Master income ID negative when adding to {userId} of {serverId}");
                return false;
            }

            Income incomeDetails = await ReadIncome(masterIncomeId); // Now returns Income?
            if (incomeDetails.IncomeId == 0) // Check for null
            {
                Console.WriteLine($"Error: Master income with ID {masterIncomeId} not found. Cannot add to user.");
                return false;
            }
       
            // Adding in item for user if there is one
            if (incomeDetails.ItemSourceId.HasValue)
            {
                await AddItemToUser(userId, incomeDetails.ItemSourceId.Value, serverId);
            }

            string insertSql = "INSERT INTO incomes (income_name, user_id, server_id, master_income_id, last_claimed_timestamp, amount, percent, item_source_id) " +
                               "VALUES (@incomeName, @userId, @serverId, @masterIncomeRefId, @lastClaimedTimestamp, @amount, @percent, @itemSourceId)";

            using (MySqlCommand cmd = new MySqlCommand(insertSql, _serverConnection))
            {
                cmd.Parameters.AddWithValue("@incomeName", incomeDetails.IncomeName); // From master details
                cmd.Parameters.AddWithValue("@userId", userId);
                cmd.Parameters.AddWithValue("@serverId", serverId);
                cmd.Parameters.AddWithValue("@masterIncomeRefId", masterIncomeId);
                cmd.Parameters.AddWithValue("@lastClaimedTimestamp", DBNull.Value); // New income, no claim yet
                cmd.Parameters.AddWithValue("@amount", incomeDetails.Amount); // From master details
                cmd.Parameters.AddWithValue("@percent", incomeDetails.Percent); // From master details

                // Handle nullable ItemSourceId
                cmd.Parameters.AddWithValue("@itemSourceId", incomeDetails.ItemSourceId.HasValue ? incomeDetails.ItemSourceId.Value : DBNull.Value);

                int rowsAffected = await cmd.ExecuteNonQueryAsync();
                return rowsAffected > 0;
            }


        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error when adding income {masterIncomeId} to user {userId} of {serverId}: {ex.Message}");
            Console.WriteLine(ex.ToString());
            return false;
        }
        
    }

    public async Task<Income> ReadIncome(int incomeId)
    {
        EnsureConnectionOpen(); 
        try
        {
            string sql = "SELECT income_id, cooldown, income_name, percent, amount, item_source_id FROM master_incomes WHERE income_id = @incomeId";
            using (MySqlCommand cmd = new MySqlCommand(sql, _serverConnection))
            {
                cmd.Parameters.AddWithValue("@incomeId", incomeId);

                using (MySqlDataReader reader = (MySqlDataReader)await cmd.ExecuteReaderAsync())
                {
                    if (reader.Read())
                    {
                        // Get item nullable item source
                        int? itemSourceId;
                        int itemSourceIdIndex = 5; // Index on database is 6
                        if (reader.IsDBNull(itemSourceIdIndex)) { itemSourceId = null; }
                        else { itemSourceId = reader.GetInt32("item_source_id"); }

                        // Read cooldown as long (total seconds) and convert to TimeSpan
                        long cooldownSeconds = reader.GetInt64("cooldown"); // <--- Read as long
                        TimeSpan cooldown = TimeSpan.FromSeconds(cooldownSeconds); // <--- Convert to TimeSpan

                        return new Income(
                            incomeId: reader.GetInt32("income_id"),
                            incomeName: reader.GetString("income_name"),
                            amount: reader.GetInt64("amount"),
                            percent: reader.GetBoolean("percent"),
                            cooldown: cooldown,
                            itemSourceId: itemSourceId
                        );

                    }
                    else
                    {
                        Console.WriteLine($"Reading error reading income {incomeId}");
                        return new Income();
                    }
                }
            }
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error reading income {incomeId}: {ex.Message}");
            Console.WriteLine(ex.ToString());
            return new Income();
        }
            
        
    }

    public async Task<int> GetMasterIndexFromIncomeName(string incomeName)
    {
        EnsureConnectionOpen(); 
        try
        {
            string sql = "SELECT income_id FROM master_incomes WHERE income_name = @incomeName";

            using (MySqlCommand cmd = new MySqlCommand(sql, _serverConnection))
            {
                cmd.Parameters.AddWithValue("@incomeName", incomeName);
                object result = await cmd.ExecuteScalarAsync();

                if (result != null && result != DBNull.Value)
                {
                    return Convert.ToInt32(result);
                }
                else
                {
                    return -1;
                }
            }
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error retrieving master income Id for '{incomeName}': {ex.Message}");
            Console.WriteLine(ex.ToString());
            return -1;
        }
        
    }
    

    public async Task<bool> ClearDatabaseForTestingAsync(bool resetAutoIncrement = true)
    {
        EnsureConnectionOpen(); 
        List<string> tablesToClear = new List<string>
        {
            "inventories",
            "transactions",
            "users",
            "master_items",
            "incomes", // Assuming this is your user-specific income table
            "master_incomes", // Assuming this is your global income definition table
            "servers"
        };

        
        
        MySqlTransaction transaction = null;

        try
        {
            // Start a transaction for the entire clear operation
            transaction = _serverConnection.BeginTransaction();
            Console.WriteLine("\n--- Clearing Database for Testing ---");

            // 1. Temporarily disable foreign key checks
            Console.WriteLine("Disabling foreign key checks...");
            using (MySqlCommand disableFkCmd = new MySqlCommand("SET FOREIGN_KEY_CHECKS = 0;", _serverConnection, transaction))
            {
                await disableFkCmd.ExecuteNonQueryAsync();
            }

            // 2. Perform deletions
            foreach (string tableName in tablesToClear)
            {
                string deleteSql = $"DELETE FROM `{tableName}`"; // Use backticks for table names
                Console.WriteLine($"Deleting from table: {tableName}...");
                using (MySqlCommand cmd = new MySqlCommand(deleteSql, _serverConnection, transaction))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                if (resetAutoIncrement)
                {
                    string resetAutoIncrementSql = $"ALTER TABLE `{tableName}` AUTO_INCREMENT = 1";
                    Console.WriteLine($"Resetting AUTO_INCREMENT for {tableName}...");
                    using (MySqlCommand cmd = new MySqlCommand(resetAutoIncrementSql, _serverConnection, transaction))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
            }

            // 3. Re-enable foreign key checks
            Console.WriteLine("Re-enabling foreign key checks...");
            using (MySqlCommand enableFkCmd = new MySqlCommand("SET FOREIGN_KEY_CHECKS = 1;", _serverConnection, transaction))
            {
                await enableFkCmd.ExecuteNonQueryAsync();
            }

            // Commit the entire transaction
            transaction.Commit();
            Console.WriteLine("Database cleared successfully for testing!");
            return true;
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Error clearing database: {ex.Message}");
            Console.WriteLine(ex.ToString());
            // Rollback the transaction on error, which will also undo the SET FOREIGN_KEY_CHECKS = 0
            transaction?.Rollback();
            // IMPORTANT: If rollback fails to re-enable FK checks, you might need to manually run SET FOREIGN_KEY_CHECKS = 1; in phpMyAdmin
            Console.WriteLine("WARNING: Foreign key checks might still be disabled if rollback failed. Check phpMyAdmin.");
            return false;
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
            await dbManager.ConnectAsync();
            await dbManager.ClearDatabaseForTestingAsync();
            Console.WriteLine("------ TESTING BEGIN ------");
            
            // Start stopwatch
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            // Ensure global economy and user are set up
            await dbManager.InsertServerAsync("1", "Global Economy");
            await dbManager.InsertUserAsync("1", "1", "Global User");

            // Start the user and server
            await dbManager.TestConnectionAsync();
            await dbManager.InsertServerAsync("1234", "The squad");
            await dbManager.InsertUserAsync("1234", "1234", "Matercan");
            int userIndex = await dbManager.GetDatabaseUserIdFromDiscordUserId("1234", "1234");

            // Add in money
            Console.WriteLine($"User 1234 balance: {await dbManager.GetUserBalancesAsync("1234", "1234")}");
            await dbManager.AddCashToUserAsync("1234", "1234", 100, DatabaseManager.TransactionType.income, "test");
            await dbManager.AddBankToUserAsync("1234", "1234", 1000, DatabaseManager.TransactionType.sell, "test");
            Console.WriteLine($"User 1234 balance: {await dbManager.GetUserBalancesAsync("1234", "1234")}");

            // Items
            await dbManager.InitializeItem(true, "test", 500, false);
            await dbManager.InitializeItem(false, "knife", 1000, false, command: "Stab");
            int itemIndex = await dbManager.GetMasterIndexFromItemName("test");
            // await dbManager.AddItemToUser(userIndex, itemIndex, "1234");
            // itemIndex = await dbManager.GetMasterIndexFromItemName("knife");

            // Incomes
            // await dbManager.InitializeIncome("crime", 5, true, new TimeSpan(0, 6, 0, 0), itemSourceId: itemIndex);
            int incomeIndex = await dbManager.GetMasterIndexFromIncomeName("crime");
            // await dbManager.AddIncomeToUser(userIndex, incomeIndex, "1234");

            // Everything test
            await dbManager.InitializeIncome("test1", 500, false, TimeSpan.Zero);
            incomeIndex = await dbManager.GetMasterIndexFromIncomeName("test1");
            
            await dbManager.InitializeItem(true, "test1", 500, false, incomeSourceId: incomeIndex);
            itemIndex = await dbManager.GetMasterIndexFromItemName("test1");

            await dbManager.InitializeIncome("test2", 5, true, TimeSpan.Zero, itemSourceId: itemIndex);
            incomeIndex = await dbManager.GetMasterIndexFromIncomeName("test2");

            await dbManager.InitializeItem(true, "test2", 1000, true, incomeSourceId: incomeIndex);
            itemIndex = await dbManager.GetMasterIndexFromItemName("test2");

            await dbManager.AddItemToUser(userIndex, itemIndex, "1234");

            // End stopwatch
            stopwatch.Stop();
            Console.WriteLine($"Tasks completed in {stopwatch.ElapsedMilliseconds}ms");
        }
    }
}
