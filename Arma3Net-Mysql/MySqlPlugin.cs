using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Arma3Net_Mysql
{
    /// <summary>
    /// Class for interacting with a MySQL database
    /// </summary>
    [AddIn("mysql", Version="0.0.0.1", Publisher="UKE Edgecrusher", Description="Change Me")]
    public class MySqlPlugin
    {
        #region private variables

        /// <summary>
        /// Store the Date/Time when this class was instanciated
        /// </summary>
        private DateTime _activated = DateTime.MinValue;


        /// <summary>
        /// Dictonary for all active connections
        /// </summary>
        private Dictionary<int, MySqlConnection> _condic = new Dictionary<int, MySqlConnection>();


        /// <summary>
        /// Dictonary for "tooLong" requests
        /// </summary>
        private Dictionary<int, string> _datadic = new Dictionary<int, string>();

        /// <summary>
        /// The max size of the header data.
        /// The value minimize the payload data size
        /// </summary>
        private const int HeaderSize = 96;

        #endregion

        #region constructors

        /// <summary>
        /// Constructor
        /// </summary>
        public MySqlPlugin()
        {
            // set local marker for time instanciated
            this._activated = DateTime.Now;
        }

        #endregion

        /// <summary>
        /// Open a database connection with parameters from the "default" connection defined in configuration file
        /// </summary>
        /// <returns>An object Array (object[]). First elements holds a bool value of true for success and false if there was an error. The second element holds NULL on success and error information text (string) on failure.</returns>
        [UnformattedResult]
        public object[] Open()
        {
            ConfigEntry ce;
            try
            {
                ce = ConfigReader.GetConfigEntry();
            }
            catch (Exception ex)
            {
                return new object[] { false, ex.Message.ToString() };
            }
            return Open(ce.Server, ce.Database, ce.Username, ce.Password);
        }


        /// <summary>
        /// Open a database connection with parameters from the configuration file identified by the name attribute of the connection node
        /// </summary>
        /// <param name="ConfigEntryName">Value of the name attribute of a connection element in the configfile</param>
        /// <returns>An object Array (object[]). First elements holds a bool value of true for success and false if there was an error. The second element holds NULL on success and error information text (string) on failure.</returns>
        [UnformattedResult]
        public object[] Open(String ConfigEntryName)
        {
            ConfigEntry ce;
            try
            {
                ce = ConfigReader.GetConfigEntry(ConfigEntryName);
            }
            catch (Exception ex)
            {
                return new object[] { false, ex.Message.ToString() };
            }
            return Open(ce.Server, ce.Database, ce.Username, ce.Password);
        }


        /// <summary>
        /// Opens a database connection
        /// This should be the first call before calling any other function in this class
        /// </summary>
        /// <param name="server">Servername or IP of the database server</param>
        /// <param name="dbname">Databasename to connect</param>
        /// <param name="user">Username for connection credentials</param>
        /// <param name="password">Password for connection credentials</param>
        /// <returns>An object Array (object[]). First elements holds a bool value of true for success and false if there was an error. The second element holds NULL on success and error information text (string) on failure.</returns>
        public object[] Open(string server, string dbname, string user, string password)
        {
            MySqlConnection tempcon = new MySqlConnection();

            MySqlConnectionStringBuilder conBuilder = new MySqlConnectionStringBuilder();
            conBuilder.Server = server;
            conBuilder.Database = dbname;
            conBuilder.UserID = user;
            conBuilder.Password = password;

            tempcon.ConnectionString = conBuilder.ConnectionString;

            // free conBuilder
            conBuilder = null;

            try
            {
                tempcon.Open();
            }
            catch (Exception ex)
            {
                return new object[] { 0, ex.Message.ToString() };
            }

            // get a valid key
            int newid = GetConnectionID();

            // add the connection to the connection dictonary
            _condic.Add(newid, tempcon);

            return new object[] { 1, newid };
        }


        /// <summary>
        /// Close the current database connection if there is one.
        /// This should be the last call
        /// </summary>
        /// <returns></returns>
        public object[] Close(int id)
        {

            MySqlConnection tempcon;

            // check if key exist connection dictionary
            if (!_condic.Keys.Contains(id))
            {
                return new object[] { false, String.Format("Given connection id ({0}) invalid!", id) };
            }

            tempcon = _condic[id];


            if (tempcon != null)
            {
                // close non closed connection
                if (tempcon.State != ConnectionState.Closed)
                {
                    try
                    {
                        tempcon.Close();
                    }
                    catch (Exception ex)
                    {
                        // cleanup
                        tempcon = null;

                        // remove entry form connection dictonary
                        _condic.Remove(id);

                        return new object[] { 0, "Exception: " + ex.Message };
                    }

                }
                // cleanup
                tempcon.Dispose();
                tempcon = null;

                // remove entry form connection dictonary
                _condic.Remove(id);

                return new object[] { 1, id };

            }
            else
            {
                return new object[] { 0, "Non existing connection!" };
            }


        }


        /// <summary>
        /// Close all opened connections
        /// </summary>
        /// <returns>
        /// object[] { true , count closed connections };
        /// object[] { false, exception message };
        /// </returns>
        public object[] CloseAll()
        {
            int counter = 0;

            try
            {
                foreach (KeyValuePair<int, MySqlConnection> kv in _condic)
                {
                    if (kv.Value.State != ConnectionState.Closed)
                    {
                        kv.Value.Close();
                    }
                    kv.Value.Dispose();
                    counter += 1;
                }

                _condic.Clear();

                return new object[] { 1, counter };
            }
            catch (Exception ex)
            {
                return new object[] { 0, String.Format("Exception: {0}", ex.Message) };
            }
        }


        /// <summary>
        /// Executes the given query
        /// </summary>
        /// <param name="id">Connection id from open command</param>
        /// <param name="query">Select/Insert/Update/Delete SQL Command</param>
        /// <param name="maxResultSize">maximum data size returnable to the game</param>
        /// <returns>Select: the data rows; Insert/Update/Delete: the affected row count</returns>
        [MaxResultSize]
        [UnformattedResult]
        public string Exec(int id, string query, int maxResultSize)
        {
            // check if key exist connection dictionary
            if (!_condic.Keys.Contains(id))
            {
                return Format.ObjectAsSqf(new object[] { false, String.Format("Given connection id ({0}) is invalid!", id) });
            }

            // check if there is a valid connection object
            if (_condic[id] == null)
            {
                return Format.ObjectAsSqf(new object[] { false, "No connection. Internal error." });
            }

            // check if the connection is open
            if (_condic[id].State != System.Data.ConnectionState.Open)
            {
                return Format.ObjectAsSqf(new object[] { false, String.Format("Connection not open. Current state: {0}", _condic[id].State.ToString()) });
            }

            MySqlDataReader reader;

            try
            {
                // excute the statement
                reader = (new MySqlCommand(query, _condic[id])).ExecuteReader();
            }
            catch (Exception ex)
            {
                return Format.ObjectAsSqf(new object[] { false, "Exception: " + ex.Message.ToString() });
            }

            object Result;

            if (reader.HasRows)
            {
                // there are datarows to return
                System.Collections.ArrayList ResultSet = new System.Collections.ArrayList();
                object[] cols = new object[reader.FieldCount];

                while (reader.Read())
                {
                    // object[] cols = new object[reader.FieldCount];
                    int count = reader.GetValues(cols);
                    ResultSet.Add(cols);
                }

                // convert ArrayList to an object[] with .ToArray()
                Result = ResultSet.ToArray();
            }
            else
            {
                // no data rows, so return affected rows
                Result = reader.RecordsAffected;
            }

            // cleanup reader object
            reader.Close();
            reader.Dispose();
            reader = null;

            string ResultString = Format.ObjectAsSqf(Result);

            // calculate maxPayloadSize
            int maxPayloadSize = maxResultSize - HeaderSize;

            if (ResultString.Length > maxPayloadSize)
            {
                int RequestID = GetRequestID();

                // get first payload chunk
                string payload = ResultString.Substring(0, maxPayloadSize);

                // save rest of payload in data dictonary
                _datadic.Add(RequestID, ResultString.Substring(maxPayloadSize));

                // calculate how many calls will be needed to get the other data chunks
                int moredata = (int)Math.Ceiling((double)_datadic[RequestID].Length / maxPayloadSize);

                return String.Format("[1,{0},{1},{2}]", moredata.ToString(), RequestID, payload);
            }

            return String.Format("[1,0,-1,{0}]", ResultString);
        }


        /// <summary>
        /// Get the next chunk of a request
        /// </summary>
        /// <param name="requestID">The request ID, given from first "EXEC" call</param>
        /// <param name="maxResultSize">maximum data size returnable to the game</param>
        /// <returns>The next data chunk</returns>
        [MaxResultSize]
        [UnformattedResult]
        public string Exec(int requestID, int maxResultSize)
        {
            if (!_datadic.Keys.Contains(requestID))
            {
                return "[ 0, 'Request ID not valid' ]";
            }

            int maxPayloadSize = maxResultSize - HeaderSize;

            string payload = "";

            if (_datadic[requestID].Length <= maxPayloadSize)
            {
                // this will be the last chunk
                payload = _datadic[requestID];
                _datadic.Remove(requestID);

                return String.Format("[1,0,{0},{1}]", requestID, payload);
            }

            int moredata = (int)Math.Ceiling((double)_datadic[requestID].Length / maxPayloadSize);

            payload = _datadic[requestID].Substring(0, maxPayloadSize);
            _datadic[requestID] = _datadic[requestID].Substring(maxPayloadSize);

            return String.Format("[1,{0},{1},{2}]", moredata - 1, requestID, payload);
        }


        /// <summary>
        /// Gets the state of the db connection
        /// </summary>
        /// <returns>object[]</returns>
        public object[] GetState(int id)
        {
            MySqlConnection tempcon;

            // check if key exist connection dictionary
            if (!_condic.Keys.Contains(id))
            {
                return new object[] { 0, String.Format("Given connection id ({0}) invalid!", id) };
            }

            tempcon = _condic[id];
            if (tempcon == null)
            {
                return new object[] { 0, "NoConnection" };
            }

            return new object[] { 1, tempcon.State.ToString() };
        }

        /// <summary>
        /// Gets the state of the db connection
        /// </summary>
        /// <returns>object[]</returns>
        public object[] GetStateAll()
        {
            // check if any connection exists
            if (_condic.Count == 0)
            {
                return new object[] { 1, new object[] { } };
            }

            List<object[]> states = new List<object[]>();

            // build an object array with the states of the connections
            foreach (var item in _condic)
            {
                states.Add(new object[] { item.Key, item.Value.State.ToString() });
            }

            return new object[] { 1, states };
        }

        /// <summary>
        /// Get information about this function(s)
        /// </summary>
        /// <returns>Informations about this functions</returns>
        public object[] GetSyntax()
        {
            string directory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            string file = Assembly.GetExecutingAssembly().GetName().Name;

            string fileName = Path.Combine(directory, file + ".help.txt");

            if (!File.Exists(fileName))
            {
                return new object[] { 0, String.Format("No information available.\r\n File not found: {0}", fileName) };
            }

            return new object[] { 1, File.ReadAllText(fileName, System.Text.Encoding.UTF8) };
        }


        /// <summary>
        /// Retrieves the time when this addin was activated (instanciated)
        /// Value return UTC Time in invariant culture.
        /// </summary>
        /// <returns>UTC-Time in Format "MM-DD-YYY HH:MM:SS"</returns>
        public object[] GetActivated()
        {
            // Format "MM-DD-YYY HH:MM:SS"
            return new object[] { 1, this._activated.ToUniversalTime().ToString(CultureInfo.InvariantCulture) };
        }

        /// <summary>
        /// Get count of opened connecitons
        /// </summary>
        /// <returns>object[] { true, count }</returns>
        public object[] GetConnectionCount()
        {
            return new object[] { 1, _condic.Count };
        }


        #region private functions

        private int GetConnectionID()
        {
            if (_condic.Keys.Count == 0)
            {
                return 1;
            }

            return _condic.Keys.Max() + 1;
        }


        private int GetRequestID()
        {
            if (_datadic.Keys.Count == 0)
            {
                return 1;
            }

            return _datadic.Keys.Max() + 1;
        }


        //private string AddInName() {
        //    if (this.GetType().IsDefined(typeof(AddInAttribute), true))
        //    {

        //        object[] attribs = this.GetType().GetCustomAttributes(typeof(AddInAttribute), true);
        //        if (attribs != null && attribs.Length == 1)
        //        {
        //            return ((AddInAttribute)attribs.ElementAt(0)).Name;
        //        }
        //    }
        //    return null;
        //}

        /*
        //
        // This function is jet not implemented
        // 
        
        private string GetAddInName()
        {
            // Using reflection.
            System.Attribute[] attrs = System.Attribute.GetCustomAttributes(this.GetType());  // Reflection.

            // Displaying output.
            foreach (System.Attribute attr in attrs)
            {
                if (attr is AddInAttribute)
                {
                    AddInAttribute aia = (AddInAttribute)attr;
                    return aia.Name;
                }
            }
            throw new System.Exception("Cant get attribute name");
        }
        */

        #endregion
    }
}
