/**
 * @Author	:	Jitendra Zaa
 * @Date	: 	May 2
 * @Desc	: 	Use this class to decouple actual AsyncApexFramework, in absense 
 *          :   of this class, we would not be allowed to update AsyncApexFramework
 *          :   class, as it will give an error saying jobs for this class are  
            :   pending 
 *          :   in queue and therefore class cannot be modified
 */
public abstract class AsyncApexFrameworkScheduler implements Schedulable { }
