using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.IO;
using System.Diagnostics;

namespace UserDeleteCleaner
{
    public class ServiceBusSubscriber
    {
        NamespaceManager namespaceManager;

        string[] connectionStrings = new string[] {
		"specify endpoint string here"
        };

        string topicPath;
        string subscriptionName;
        string connectionString;
        int hostIndex;
        UserDeleteRepository userDeleteRepository;

        public ServiceBusSubscriber(string topicPath, string subscriptionName, int hostIndex)
        {
            this.topicPath = topicPath;
            this.subscriptionName = subscriptionName;
            this.connectionString = connectionStrings[hostIndex];
            this.namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            this.hostIndex = hostIndex;
            this.userDeleteRepository = new UserDeleteRepository(hostIndex);
        }

        ~ServiceBusSubscriber()
        {
            //DeleteSubscription();
        }

        public async Task Start()
        {
            OnMessageOptions options = ConfigureCallbackOptions();
            SubscriptionClient client = SubscribeTopic();
            ProcessQueuedMessages(client);
            await ListenForTopicMessages(client, options);
        }

        private OnMessageOptions ConfigureCallbackOptions()
        {
            OnMessageOptions options = new OnMessageOptions();
            options.AutoComplete = false;
            options.AutoRenewTimeout = TimeSpan.FromMinutes(1);
            return options;
        }

        private SubscriptionClient SubscribeTopic()
        {
            CreateSubscription();
            return SubscriptionClient.CreateFromConnectionString(connectionString, topicPath, subscriptionName);
        }

        private void CreateSubscription()
        {
            try
            {
                if (!namespaceManager.SubscriptionExists(topicPath, subscriptionName))
                {
                    namespaceManager.CreateSubscription(topicPath, subscriptionName);
                } else
                {
                    Debug.WriteLine("**** Subscription Exists ****");
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine("**** Error CreateSubscription " + e.ToString() + " ****");
                throw;
            }

        }

        private async void ProcessQueuedMessages(SubscriptionClient client)
        {
            try
            {
                MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
                MessageReceiver messageReceiver = factory.CreateMessageReceiver(SubscriptionClient.FormatDeadLetterPath(topicPath, subscriptionName));
                IEnumerable<BrokeredMessage> messageSet = messageReceiver.ReceiveBatch(10000);
                Debug.WriteLine("**** Received " + messageSet.Count() + " messages *****");
                foreach (BrokeredMessage message in messageSet)
                {
                    try
                    {
                        await processMessage(message);
                        Debug.WriteLine("**** ProcessQueuedMessages.processMessage completed ****");
                    }
                    catch (Exception e)
                    {
                        Debug.WriteLine("**** Error ProcessQueuedMessages.processMessage " + e.ToString() + " ****");
                    }
                }
            } catch
            {
                Debug.WriteLine("**** Error ProcessQueuedMessages ****");
            }
        }

        private async Task ListenForTopicMessages(SubscriptionClient client, OnMessageOptions options)
        {
            try
            {
                client.OnMessageAsync(async (message) =>
                {
                    try
                    {
                        await processMessage(message);
                    }
                    catch (Exception e)
                    {
                        Debug.WriteLine("**** Error ListenForTopicMessages.processMessage " + e.ToString() + " ****");
                    }
                });
            }
            catch (Exception e)
            {
                Debug.WriteLine("**** Error ListenForTopicMessages " + e.ToString() + " ****");
                throw;
            }
        }

        private void DeleteSubscription()
        {
            try
            {
                if (namespaceManager.SubscriptionExists(topicPath, subscriptionName))
                {
                    namespaceManager.DeleteSubscription(topicPath, subscriptionName);
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine("**** Error DeleteSubscription " + e.ToString() + " ****");
            }
        }

        private async Task processMessage(BrokeredMessage message)
        {
            Stream bodyStream = message.GetBody<Stream>();
            string userId = string.Empty;
            using (StreamReader reader = new StreamReader(bodyStream, Encoding.UTF8))
            {
                userId = await reader.ReadToEndAsync();
            }

            if (!string.IsNullOrEmpty(userId))
            {
                try
                {
                    await userDeleteRepository.SaveUserDeleteAsync(userId);

                    try
                    {
                        // complete will remove message from queue
                        await message.CompleteAsync();
                        Debug.WriteLine("**** processMessage Message Completed for " + userId + " ****");
                    }
                    catch (Exception e)
                    {
                        Debug.WriteLine("**** processMessage Error CompleteAsync for " + userId + " " + e.ToString() + " ****");
                    }
                }
                catch (Exception e)
                {
                    Debug.WriteLine("**** Error processMessage userDeleteRepository.SaveUserDeleteAsync for " + userId + " " + e.ToString() + " ****");
                }

            }
            else
            {
                Debug.WriteLine("**** Error processMessage No UserId ****");
            }
        }

    }
}
