import akka.actor.ActorSystem
import scala.collection.mutable.ListBuffer
import akka.actor.Props
import akka.actor.Actor
import akka.routing.RoundRobinRouter
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala
import scala.util.control.Breaks._
import java.security.MessageDigest
import scala.util.Random
import akka.actor.ActorRef
import akka.dispatch.ExecutionContexts
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

case class pushsumconvergence(difference:Double)
case class Implementpushsum(w:Integer)
case class touchedmessage(workerId:Integer)
case class StartWorking(network:String,algorithm:String)
case class getmessage(message:String)
case class startpropogation(message:String)
case class pushsum(Sreceived:Double,Wreceived:Double)
case class terminationmessage(i:Integer)
object main extends App{
  
   val master=ActorSystem("Gossp").actorOf(Props(new Master(args(0).toInt, args(1), args(2) )),name="Master")         
}
class Master(num:Integer,network:String,algorithm:String) extends Actor {
    var NodeStore: Array[ActorRef]=new Array[ActorRef](num)
    var convergancearray: Array[Integer]=new Array[Integer](num)
    var starttime:Long=0
    var convergancecount=0
    
    /////     THis part converts the number into next cubic number to make a 3d structure ////
                     var z=0;
                     breakable{ 
                        while(true){
                             z=z+1
                              if((z*z*z)>=num)
                                break
                               }
                          }
                     
                     
       ////// DEfining the variables and intitializing arrays and arraybuffers to check convergance
       //////  for the topologies and assigning the 3d new size.              
               var threeDCount=z*z*z
               var ThreenodeStore: Array[ActorRef]=new Array[ActorRef](threeDCount)
               var Threeconvergance:Array[Integer]=new Array[Integer](threeDCount)
               var Listforfull = new ArrayBuffer[Integer]
                       for(neighbourCount:Int <- 0 to num-1)
                                   {
                                     Listforfull+=neighbourCount
                                    }
               var convergedline = new Array[Integer](num)
                    for(i:Int <- 0 to num-1)
                       {
                         convergedline(i)=1
                       }
                var convergedthreed = new Array[Integer](threeDCount)
                    for(i:Int <- 0 to threeDCount-1)
                       {
                         convergedthreed(i)=1
                       }
               
               
               
               
               for(i<-0 to threeDCount-1) Threeconvergance(i)=0
               
               
               
     ///////// Creating workers separately for 3d according to its size///////          
    if(network.equalsIgnoreCase("3D")||network.equalsIgnoreCase("imp3D")){
                     
                     for(i<-0 to z-1){
                       for(j<-0 to z-1){
                         for(k<-0 to z-1){
                           val count=i*(z*z)+j*z+k
                           ThreenodeStore(count)=context.system.actorOf(Props(new Worker(count,self,NodeStore,ThreenodeStore,network,algorithm,threeDCount,z,Listforfull,convergedline,convergedthreed)),"actor"+count.toString)
                           
                         }
                       }
                       
                     }
    }
    ///////////// creating worker for line and full topologies//////////           
    else{
    for(i<- 0 to num-1){
     NodeStore(i)=context.system.actorOf(Props(new Worker(i,self,NodeStore,ThreenodeStore,network,algorithm,num,z,Listforfull,convergedline,convergedthreed)),"actor"+i.toString)
    }
    }
               //////Checking for convergance and termination of the network./////
    def receive={
                          case touchedmessage(i:Integer) =>
                               if(network.equalsIgnoreCase("3D")||network.equalsIgnoreCase("imp3D")){
                                convergancecount=convergancecount+1;
                                println("Convergance message from"+i)
                                if(convergancecount==threeDCount-2){
                               println("Every node converged,ie got message once")
                                val endTime = System.currentTimeMillis()
                                
                                println("\n\n\t*******Time taken for convergence : " + (endTime-starttime) + " milliseconds*******\n\n")
                                System.exit(1)
                               }
                                }
                               else{
                                 convergancecount=convergancecount+1;
                                println("Convergance message from"+i)
                                if(convergancecount==num){
                               println("Every node converged,ie got message once")
                                val endTime = System.currentTimeMillis()
                                println("\n\n\tTime taken for convergence : " + (endTime-starttime) + " milliseconds\n\n")
                                System.exit(1)
                                }
                                
                                }
    
                          case terminationmessage(i:Integer) =>
                            println(".......................................Termination message from node: "+i)
                            
        //////////// Checking convergance for push sum////////////////
                 case pushsumconvergence(difference:Double)=>
                        println("Time taken for convergence is: "+(System.currentTimeMillis()-starttime)+"milisec")
                        println("Ratio finally is "+difference+" which is equal to average")
                        println("the number of nodes are rounded off to perfect cube larger,so ratio is half of that cube.\n")
                        System.exit(1)       
                 
    }
             //////////// Sending messages to workers according to the algorithm ////////
                               if(algorithm.equalsIgnoreCase("gossip"))
                                 {
                                    var message:String="God is dead"
                                    starttime=System.currentTimeMillis()
                                    if(network.equalsIgnoreCase("3D")||network.equalsIgnoreCase("imp3D")){
                                       ThreenodeStore(0)! startpropogation(message)
                                    }
                                    else
                                    {
                                     NodeStore(num/2)!startpropogation(message)
                                    }
                                  }

                                if(algorithm.equalsIgnoreCase("pushsum")){
                                     starttime=System.currentTimeMillis()
                                        if(network.equalsIgnoreCase("3D")||network.equalsIgnoreCase("imp3D")){
                                           ThreenodeStore(2)! pushsum(2.0,1)
                                         }
                                        else
                                         {
                                           NodeStore(2)! pushsum(2.0,1)
                                         }
                                   }
   
     
                                 }
class Worker(workerId:Integer,master:ActorRef,NodeStore:Array[ActorRef],ThreenodeStore:Array[ActorRef],network:String,algorithm:String,num:Integer,edge:Integer,Listforfull:ArrayBuffer[Integer],convergedline:Array[Integer],convergedthreed:Array[Integer]) extends Actor{
  
  var convergancecount=0;
  var neighbour:Array[Integer]=new Array[Integer](7)
  var randindex=0
  var Scurrent:Double=workerId.toDouble
  var Wcurrent:Double=1
  var converged=false
  var terminumber=0
  var nextWorkerCount = 0
  var neighbourr =0
  def receive = {
    
    case startpropogation(message:String)=>
      
                 ///// Checking if the node has already converged,Keeping a count of convergance,
                 ////// If a node is visited once,it is shown to be  touched,or say it participated 
                 //////  in convergance of the network. A message is sent to master only once and so 
                 /////   if count becomes as large as size of the network network is converged. 
                 if(!converged )
                  {
                    if(convergancecount==0)
                        {
                          master ! touchedmessage(workerId)
                        }
                       ///// Drop the worker from the list once it terminates. 3d case is handled 
                       /////  differently as the worker count changes.
                         if(convergancecount ==10)
                        {
                         converged =true
                        Listforfull.drop(workerId)
                        if(network.equalsIgnoreCase("line")||network.equalsIgnoreCase("full"))
                        convergedline(workerId)=0
                        convergedthreed(workerId)=0
                        master ! terminationmessage(workerId)
                        }
                /////////// Different networks are handled in a different manner/////
                  if(network.equalsIgnoreCase("line")){
              
                         if(workerId==0)
                                {
                                    convergancecount =convergancecount+1
                                   NodeStore(1) ! startpropogation(message)
                                }
                          else if (workerId==NodeStore.length-1)
                                {  
                                   convergancecount =convergancecount+1
                                   NodeStore(NodeStore.length-2) !startpropogation(message)
                                }
                          else
                                {
                                   nextWorkerCount =  Random.nextInt(2)
    
                                   if(nextWorkerCount==0)
                                   neighbourr = workerId+1
                          else
                                   neighbourr = workerId-1
                  ///////////  If the neighbour is still alive and if the node is not a converged one then 
                          //     select a node from list and send it the message
                          if((convergedline(neighbourr)==1)&&(!converged))
                                {
                                    if(!sender.equals(self))
                                       {
                                    convergancecount =convergancecount+1
                                }
                  
                                    NodeStore(neighbourr) ! startpropogation(message)
                                }
                           }
           
                    /////// Using scheduleonce makes a node schedule once before terminating,the node 
                    /////// will propogate a message 20ms after terminating which will keep the message alive
                                  import context.dispatcher
                                 val delayTime = scala.concurrent.duration.FiniteDuration(20, "milliseconds")
                                  context.system.scheduler.scheduleOnce(delayTime, self, startpropogation(message))
                           // }
              
                       }
                 if(network.equalsIgnoreCase("full")){
                               randindex=scala.util.Random.nextInt(Listforfull.length)
                                if((Listforfull.length==1)||(randindex!=workerId))
                                {
                                  
                                    if(!sender.equals(self))
                                       {
                                    convergancecount =convergancecount+1
                                }
                  
                                    NodeStore(randindex) ! startpropogation(message)
                                } 
                               import context.dispatcher
                                 val delayTime = scala.concurrent.duration.FiniteDuration(20, "milliseconds")
                                  context.system.scheduler.scheduleOnce(delayTime, self, startpropogation(message))
                    }
                  if(network.equalsIgnoreCase("3D")||network.equalsIgnoreCase("imp3D")){
                     
                               var neighbourThreed=new ArrayBuffer[Integer]()
                               
                               
                               
                               
                          ///// The network is defined as a cubic 3d network and so the x y z coordinates are calculated
                          ///// based on the worker id. Then the boundary conditions are handled and neighbours are chosen
                               
                               var temp=workerId
                               var x=temp/(edge*edge)
                               temp=temp%(edge*edge)
                               var y=temp/edge
                               var z= temp %edge
                                 
                               if(x>0){
                                 var temp1=x-1
                                 val p:Integer=temp1*(edge*edge)+y*edge+z
                                 neighbourThreed+=(p)
                                 
                               }
                               if(y>0){
                                 var temp1=y-1
                                 val neighbour=x*(edge*edge)+temp1*edge+z
                                 neighbourThreed+=(neighbour)
                               }
                               if(z>0){
                                 var temp1=z-1
                                 val neighbour=x*(edge*edge)+y*edge+temp1
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(x<edge-1){
                                 var temp=x+1
                                 val neighbour:Integer=temp*(edge*edge)+y*edge+z
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(y<edge-1){
                                 var temp=y+1
                                 val neighbour=x*(edge*edge)+temp*edge+z
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(z<edge-1){
                                 var temp=z+1
                                 val neighbour=x*(edge*edge)+y*edge+temp
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(network.equalsIgnoreCase("imp3D")){
                                 //println("..........................................Going inside imp3d special")
                                 val randneighb=scala.util.Random.nextInt(num-3)
                                 neighbourThreed+=(randneighb)
                               }
                               randindex=scala.util.Random.nextInt(neighbourThreed.length)
                               
                               if((convergedthreed(neighbourThreed(randindex))==1)&&(!converged))
                                {
                                    if(!sender.equals(self))
                                       {
                                    convergancecount =convergancecount+1
                                }
                                   
                                    ThreenodeStore(neighbourThreed(randindex)) ! startpropogation(message)
                                }
                               import context.dispatcher
                                 val delayTime = scala.concurrent.duration.FiniteDuration(20, "milliseconds")
                                 context.system.scheduler.scheduleOnce(delayTime, self, startpropogation(message))
                           }
                               
                               
                                                        
                               
      
    
                  }
    case pushsum(sreceived:Double,wreceived:Double) =>
                   
                   
                   
                // println("I am worker"+workerId+" and my old S and W are "+Scurrent+" and "+Wcurrent)
      
                  //Received value is taken from the sender and add to the current value
                  // and half is sent to the neighbour and half kept. Ratios are calculated and
                  // compared. Once we notice that a node is getting three consecutive values
                  // of network average, we come to know that every node has an average value 
                  // and the network terminates.
                  var oldratio=Scurrent/Wcurrent
                  Scurrent=(Scurrent+sreceived)/2
                  Wcurrent=(Wcurrent+wreceived)/2
                  var Stosend=(Scurrent+sreceived)/2
                  var Wtosend=(Wcurrent+wreceived)/2
                  var newratio=Scurrent/Wcurrent
                  if(convergancecount>=3){
                    master! pushsumconvergence(math.abs(newratio))
                  }
                  if(math.abs(oldratio-newratio) <= math.pow(10,-10)){
                    convergancecount=convergancecount+1
                    
                  }
                 // println("I am worker"+workerId+" and my new S and W are "+Scurrent+" and "+Wcurrent)
                  if(network.equalsIgnoreCase("line")){
                            if(workerId==0)
                                {
                                   NodeStore(1) ! pushsum(Stosend,Wtosend)
                                }
                          else if (workerId==NodeStore.length-1)
                                {
                                   NodeStore(NodeStore.length-2) !pushsum(Stosend,Wtosend)
                                }
                          else
                                {
                                   nextWorkerCount =  Random.nextInt(2)
    
                                   if(nextWorkerCount==0)
                                   neighbourr = workerId+1
                                   else
                                  neighbourr = workerId-1
                                    NodeStore(neighbourr) ! pushsum(Stosend,Wtosend)
                                }      
              
                       }
                 if(network.equalsIgnoreCase("full")){
                               randindex=scala.util.Random.nextInt(num-1)
                             //  println("randindex"+randindex)
                               val result=randindex
                              // println("result"+result)
                               val sendto=context.system.actorSelection("/user/actor" + result)  
                                sendto! pushsum(Stosend,Wtosend)
                    }
                 
                 ///// Please note that the number of workers actually in the 3d network is
                 ////   more than what is entered and hence the average count will be half of
                 /////// that count.
                  if(network.equalsIgnoreCase("3D")||network.equalsIgnoreCase("imp3D")){
                    
                      println("Processing ratio for 3d")
                    
                               var neighbourThreed=new ArrayBuffer[Integer]()  
                               var temp=workerId
                               var x=temp/(edge*edge)
                               temp=temp%(edge*edge)
                               var y=temp/edge
                               var z= temp %edge
                               if(x>0){
                                 var temp1=x-1
                                 val p:Integer=temp1*(edge*edge)+y*edge+z
                                 neighbourThreed+=(p)
                               }
                               if(y>0){
                                 var temp1=y-1
                                 val neighbour=x*(edge*edge)+temp1*edge+z
                                 neighbourThreed+=(neighbour)
                                 
                                 
                               }
                               if(z>0){
                                 var temp1=z-1
                                 val neighbour=x*(edge*edge)+y*edge+temp1
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(x<edge-1){
                                 var temp=x+1
                                 val neighbour:Integer=temp*(edge*edge)+y*edge+z
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(y<edge-1){
                                 var temp=y+1
                                 val neighbour=x*(edge*edge)+temp*edge+z
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(z<edge-1){
                                 var temp=z+1
                                 val neighbour=x*(edge*edge)+y*edge+temp
                                 neighbourThreed+=(neighbour)
                                 
                               }
                               if(network.equalsIgnoreCase("imp3D")){
                                 val randneighb=scala.util.Random.nextInt(num-3)
                                 neighbourThreed+=(randneighb)
                               }
                               randindex=scala.util.Random.nextInt(neighbourThreed.length)
                               val result=neighbourThreed(randindex)
                                                        
                               val sendto=context.system.actorSelection("/user/actor" + result)
                               
                                  sendto! pushsum(Stosend,Wtosend)
                  
  
                  }
    
    
  }
}