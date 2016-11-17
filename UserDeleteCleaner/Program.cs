using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace UserDeleteCleaner
{
    class Program
    {
        private static ServiceBusSubscriber userdeleteSubscriber;
        static void Main(string[] args)
        {
            Debug.WriteLine("**** UserDeleteCleaner begin ****");

            MainAsync().Wait();
    
            while (true)
            {
            }

            Debug.WriteLine("**** UserDeleteCleaner end ****");
        }

        private static async Task MainAsync()
        {
            await SubscribeToServiceBus();
        }

        private static async Task SubscribeToServiceBus()
        {
            string topicPath = "userdelete";
            string subscriptionName = "analyticsworkers";
            int hostIndex = 0; // 0 = DEV, 1 = INT, 2 = PROD, 3 = EU DEV, 4 = EU INT, 5 = EU PROD
            userdeleteSubscriber = new ServiceBusSubscriber(topicPath, subscriptionName, hostIndex);
            await userdeleteSubscriber.Start();
        }
    }
}
