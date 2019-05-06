/**
 * @Author	:	Jitendra Zaa
 * @Date	: 	May 2
 * @Desc	: 	Async Framework v 2
 * 				Framework to handle governor limits on max 100 batch class in Flex queue 
 * 				and Max 50 Queueuable classes can be invoked
 * 				1. Add Batch Apex in Custom Object to process later if limit hits
 * 				2. Add Queuable class in Custom Object to process later if limit hits
 * 				3. Collects log of batch rersults and updates record in custom object
 * 				4. Bulkified 
 * 
 * Limit of 100 job in Flex Queue
 * Framework to address limit of 100 Batch Apex in Queue
 * */
public class AsyncApexFramework extends AsyncApexFrameworkScheduler{
    private static Integer MAX_BATCHLIMIT = 100 ;
    private static Integer availableBatchLimit = null;
    private static List<AsyncQueue__c > lstBatch = new List<AsyncQueue__c >(); 
    private static Integer previousCount = -1;
    private static Integer MAX_RETRY = 2; //User Custom Label here
    
    /**
    * governor limit left for DML operations
    */
    private static Integer availableDMLLimit(){
        return Limits.getLimitDMLRows() - Limits.getDmlRows();
    }
    
    /**
    * governor limit left for Queuable Apex
    */
    private static Integer availableQueueableLimit(){
        return Limits.getLimitQueueableJobs() - Limits.getQueueableJobs() ;
    }
    
    /**
    * Use this method to submit Queueable Job
    * @queueableClassInstance - instace of Queueable Class 
    * @priority - Priority of job, pass null
    * @allowRetryOnDail - If there is any exception in job , retry should be
    *                   - attempted or not
    */
    public static String submitQueueable(Object queueableClassInstance,Integer priority , Boolean allowRetryOnFail){
        String jobId = null;
        if(priority == null ||priority == 0 ){
            priority = 99;
        }
        String s = JSON.serialize(queueableClassInstance);
        AsyncQueue__c  q = new AsyncQueue__c (); 
        q.Is_Retry__c = allowRetryOnFail; 
        q.Class_Name__c = String.valueOf(queueableClassInstance).split(':')[0];
        q.priority__c = priority ;
        q.object__c = s;
        q.Job_Type__c = 'Queueable';
        if(availableQueueableLimit() > 0 ){   
            q.Job_Id__c = jobId = System.enqueueJob((Queueable )queueableClassInstance);
            q.Status__c = 'Completed';  
        } 
        lstBatch.add(q);
        return jobId ;
    }
    
    /**
    * Use this method to submit Batch Job
    * @batchClassInstance - instace of Batch Apex Class
    * @scopeSize - scope of Batch Apex
    * @priority - Priority of job, pass null
    * @allowRetryOnDail - If there is any exception in job , retry should be
    *                   - attempted or not
    */
    public static string submitBatch(Object batchClassInstance, Integer scopeSize, Integer priority , Boolean allowRetryOnFail){
        String jobId = null;
        if(priority == null ||priority == 0 ){
            priority = 99;
        }
        if(previousCount == -1){
            AggregateResult[] groupedResults = [Select Count(Id) FROM AsyncApexJob Where Status = 'Holding'];
        	String exp0 = String.valueOf(groupedResults[0].get('expr0'));
            previousCount = Integer.valueOf(exp0) ;
        }  
        availableBatchLimit = MAX_BATCHLIMIT - previousCount;
        String s = JSON.serialize(batchClassInstance);
        AsyncQueue__c  q = new AsyncQueue__c (); 
        q.Job_Type__c = 'Batch';
        q.Batch_Size__c  = scopeSize;
        q.object__c = s;
        q.priority__c = priority ; 
        q.Is_Retry__c = allowRetryOnFail;
        q.Class_Name__c = String.valueOf(batchClassInstance).split(':')[0];

        if(availableBatchLimit > 0){  
            Database.batchable<sObject> b = (Database.batchable<sObject>)batchClassInstance; 
            q.Job_Id__c = jobId = Database.executeBatch(b, scopeSize);
            q.Status__c = 'Completed';   
        } 
        lstBatch.add(q);
        previousCount++;
        return jobId ;
    }
    
    /**
     * Call this method after all job scheduled, it will commit 
     * future jobs in custom object. Calling this method multiple 
     * time would not have any adverse effect. 
     * */
    public static void flush(){
        if(!lstBatch.isEmpty()){
            Database.insert(lstBatch,false);
            lstBatch.clear();
        }
    }
    
    /**
    *   Utility Method to get all pending jobs from custom object to process
    */
    private List<AsyncQueue__c> getPendingJobs(String jobType, Integer recordToFetch){
        return   [Select 
                  	Batch_Size__c, 
                  	object__c ,
                  	Class_Name__c,
                  	Retry_Count__c 
                  FROM 
                  	AsyncQueue__c  
                  Where 
                  	( Status__c = 'Queued'
                   	OR 
                   	    (
                            Status__c  = 'Failed'
                            AND 
                            Is_Retry__c = true 
                        )
                    )
                  	AND
                  	Job_Type__c = :jobType 
                  	AND 
                  	Retry_Count__c < : MAX_RETRY
                  	Order By 
                  	priority__c ASC
                  LIMIT 
                  	:recordToFetch ];
    }
    
    /**
    * Process all jobs added in custom object for Batch
    */
    private void startBatchJobs(List<AsyncQueue__c > lstBatch_StatusUpdate){ 
        Integer availableLimit = 0;
        AggregateResult[] groupedResults = [Select Count(Id) FROM AsyncApexJob Where Status = 'Holding'];
        String exp0 = String.valueOf(groupedResults[0].get('expr0'));
        availableLimit = MAX_BATCHLIMIT - Integer.valueOf(exp0);
        
        //We can spawn 50 async apex in single transaction
        //we can invoke 50 batch and 50 queuable from same code
        if(availableLimit > 50){
            availableLimit = 50;
        }
        
		if(availableLimit > 0){
            List<AsyncQueue__c > lstBatch = getPendingJobs('Batch', availableLimit)  ;
            if(!lstBatch.isEmpty()){
                for(AsyncQueue__c  q : lstBatch){ 
                    try{
                        if(q.Status__c == 'Failed'){
                            //If previous status is failed, increase retry count
                            q.Retry_Count__c = q.Retry_Count__c + 1;
                        }else{
                            q.Status__c = 'Completed'; 
                        }
                        Type t = Type.forName(q.Class_Name__c);
                        Object des_Obj = JSON.deserialize (q.object__c,t);
                        Database.batchable<sObject> b = (Database.batchable<sObject>)des_Obj;
                        q.Job_Id__c = Database.executeBatch(b, Integer.valueOf(q.Batch_Size__c));
                        
                    }catch(Exception e){
                        q.status__c = 'Failed'; 
                        q.note__c = e.getMessage()+'\n'+e.getStackTraceString();
                    }
                    lstBatch_StatusUpdate.add(q);
                }
            }   
        }         
    }
    
    /**
    * Process all jobs added in custom object for Queuable
    */
     private void startQueueable(List<AsyncQueue__c > lstBatch_StatusUpdate){ 
        Integer availableLimit = availableQueueableLimit() ;
        if(availableLimit > 0 ){
            List<AsyncQueue__c > lstBatch = getPendingJobs('Queueable', availableLimit)  ;
            if(!lstBatch.isEmpty()){
                for(AsyncQueue__c  q : lstBatch){ 
                    try{
                        if(q.Status__c == 'Failed'){
                            //If previous status is failed, increase retry count
                            q.Retry_Count__c = q.Retry_Count__c + 1;
                        }else{
                            q.Status__c = 'Completed'; 
                        }
                        Type t = Type.forName(q.Class_Name__c);
                        Object des_Obj = JSON.deserialize (q.object__c,t);
                        q.Job_Id__c = System.enqueueJob((Queueable )des_Obj); 
                        q.Status__c = 'Completed'; 
                    }catch(Exception e){ 
                        q.note__c = e.getMessage()+'\n'+e.getStackTraceString();
                    }
                    lstBatch_StatusUpdate.add(q);
                }
            } 
        } 
     }
    
    /**
    *   Read results of job from Salesofrce job log and update in custom object
    */
    private void collectJobInfo(List<AsyncQueue__c > lstBatch_StatusUpdate){
        Integer availableLimit = availableDMLLimit();  
        if(availableLimit > 0){ 
            List<AsyncQueue__c> lstStatusCheck = [Select 
                                                Job_Id__c,
                                                Note__c   
                                              FROM 
                                                AsyncQueue__c  
                                              Where 
                                                Error_Collection_Status__c  = 'Not Collected'
                                                AND 
                                                Status__c  IN ('Completed','Failed')   
                                              LIMIT 
                                                :availableLimit ] ;
            Map<String,AsyncQueue__c> mpJobMap = new Map<String,AsyncQueue__c>();
            for(AsyncQueue__c a : lstStatusCheck){
                if(!string.isEmpty(a.Job_Id__c)){
                    //convert 15 to 18 digit id
                    Id id15to18 = a.Job_Id__c ;
                    mpJobMap.put(id15to18,a); 
                } 
            } 

            if(mpJobMap.keyset().size() > 0){
                List<AsyncApexJob> lstJobStatus =[Select 
                                                    Id, 
                                                    ExtendedStatus, 
                                                    TotalJobItems  ,
                                                    NumberOfErrors ,
                                                    MethodName  
                                                FROM 
                                                    AsyncApexJob 
                                                Where 
                                                    Status IN ('Completed', 'Failed')  
                                                 AND ID IN : mpJobMap.keyset()]; 
                for(AsyncApexJob j : lstJobStatus){ 
                    AsyncQueue__c a = mpJobMap.get(j.Id); 
                    if(a != null){
                        String note = '';
                        note += String.isEmpty(a.Note__c)? '' : a.Note__c +'\n' ; 
                        if(!String.isEmpty(j.ExtendedStatus)){  
                            note+= String.isEmpty(j.ExtendedStatus)? '' : 'Error Messages - '+j.ExtendedStatus +'\n' ;  
                            a.Status__c = 'Failed';
                        }else{
                            a.Status__c = 'Completed'; 
                        }
                        note += String.isEmpty(j.MethodName)? '' : 'Method - '+j.MethodName +'\n'; 
                        note += 'Total Batches - '+j.TotalJobItems + '\n'; 
                        note += 'Number of Errors - '+j.NumberOfErrors ;  
                        a.Note__c = note; 
                        a.Error_Collection_Status__c = 'Collected';
                       lstBatch_StatusUpdate.add(a);     
                    }
                }
            } 
        }
    }

    /**
    *   Utility method to update status of Async Custom object records
    */
    private void saveAsyncRecords(List<AsyncQueue__c > lstBatch_StatusUpdate){
        if(!lstBatch_StatusUpdate.isEmpty()){ 
            Database.update(lstBatch_StatusUpdate,false);
         } 
        lstBatch_StatusUpdate.clear(); 
    }
    
    /**
     * Entry method by Scheduler
     * It will perform below 3 operations
     * 1. Schedule Batch if any in Queue
     * 2. Schedule Queuable if any in Queue
     * 3. Collect results of these batches from Job Queue
     * */
    public void execute(SchedulableContext SC) { 
        List<AsyncQueue__c > lstBatch_StatusUpdate = new List<AsyncQueue__c >();
		collectJobInfo(lstBatch_StatusUpdate); 
        saveAsyncRecords(lstBatch_StatusUpdate);
        startBatchJobs(lstBatch_StatusUpdate);
        startQueueable(lstBatch_StatusUpdate); 
        saveAsyncRecords(lstBatch_StatusUpdate); 
    }
} 