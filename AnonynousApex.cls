//Use below code snippet to Schedule Async Framework to run every 15 minutes and clear the queue
//Scheduler Run every 15 mins 
AsyncApexFrameworkScheduler obj1 = new AsyncApexFramework();  
AsyncApexFrameworkScheduler obj2 = new AsyncApexFramework();  
AsyncApexFrameworkScheduler obj3 = new AsyncApexFramework();  
AsyncApexFrameworkScheduler obj4 = new AsyncApexFramework();  

System.schedule('AsyncApexFramework 1', '0 0 * * * ?', obj1);
System.schedule('AsyncApexFramework 2', '0 15 * * * ?', obj2);
System.schedule('AsyncApexFramework 3', '0 30 * * * ?', obj3);
System.schedule('AsyncApexFramework 4', '0 45 * * * ?', obj4);