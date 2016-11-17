using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using System.Diagnostics;

namespace UserDeleteCleaner
{
    public class UserDeleteRepository
    {
        ConnectionMultiplexer redisConnection;

        private static string[] hosts = new string[]
        {
        "specify host string here"
        };
        private static string[] passwords = new string[] 
        {
        "speicfy password here"
        };

        public UserDeleteRepository(int hostIndex)
        {
            redisConnection = ConnectionMultiplexer.Connect(
                hosts[hostIndex] +
                ",abortConnect=false,ssl=true,password=" +
                passwords[hostIndex]);
        }

        public virtual async Task SaveUserDeleteAsync(string driverId)
        {
            try
            {
                await redisConnection.GetDatabase().StringSetAsync("datadel:" + driverId, "0", TimeSpan.FromDays(10));
                Debug.WriteLine("**** UserDelete saved to redis for " + driverId + " ****");
            }
            catch (Exception e)
            {
                Debug.WriteLine("**** UserDelete failed for " + driverId + " **** " + e.ToString());
                throw;
            }
        }
    }
}
