# sap
`sap` is a kubectl wrapper cli to perform common actions against spark applications, running using [babashka](https://github.com/babashka/babashka).

## Installation

Make sure you have [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/) install it via `brew`: `brew tap wanishing/sap` and then `brew install sap`.

## Supported Actions

* List application(s)

* Delete application(s)

* Re-apply application(s) 

* Open application UI

* Get logs of application

* Get a yaml of an application

* Gel all pods associated to application

* Cleanup of all completed applications

## Examples
* list all `predict` jobs that are running at least one day:

  ```
  ~ sap ls --prefix predict -s RUNNING --days 1
  ID                                                STATE      AGE     
  predictjob-20211004-075220989296-24ecnmskuuesn    RUNNING    1d51m41s
  predictjob-20211004-080039730731-26ttcboj7evrr    RUNNING    1d43m22s
  ```

* list all `trainingsetjob` jobs with executors count and duration:

  ```
  ~ sap ls --prefix trainingsetjob --wide
  ID                                                    STATE      AGE      DURATION   EXECUTORS 
  trainingsetjob-20211005-094136475129-2rj6velyqjjum    COMPLETED  12m24s   10m14s     62        
  trainingsetjob-20211005-094140342277-2yy6v1ystqblw    RUNNING    12m20s   12m20s     70        
  trainingsetjob-20211005-094144218354-39dj67s8topy2    RUNNING    12m17s   12m17s     72        
  trainingsetjob-20211005-094147659383-147x91jixj0o3    RUNNING    12m14s   12m14s     32  
  ```
* Port-forward UI of running application by id:
  ```
  ~ sap ls --prefix feature-population-v3
  ID                                                           STATE      AGE  
  feature-population-v3-20211005-085232553091-kkehld680swv     RUNNING    1m12s
  feature-population-v3-20211005-085235705217-sumk02v0d9bc     RUNNING    1m9s 
  feature-population-v3-20211005-085242463376-3it7p3fc4n4gs    RUNNING    1m3s 
  ~ sap ui -i kkehld680swv
  Port forwarding feature-population-v3-20211005-085232553091-kkehld680swv-driver to http://localhost:4040...
  ```

## Tips

* You can add `-v` to see all the kubectl commands being run by the tool:

  ```
  ~ sap ls -w -v
  kubectl get sparkapplication -o=jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.creationTimestamp}{"\t"}{.status.applicationState.state}{"\n"}{end}
  kubectl get pods -l spark-role=executor -o=jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.labels}{"\n"}{end}
  ID                                                           STATE        AGE          EXECUTORS 
  feature-population-v3-20211005-090345481157-ny4ypw86nmao     COMPLETED    1h13m50s     0         
  features-population-events-consumer                          RUNNING      1d3h32m9s    4         
  trainingsetjob-20211005-100854003036-cf7s5r7wgfnv            RUNNING      8m42s        112 
  ```

* When you wish to perform action given application id, you can supply partially id rather than the full one:
  ```
  ~ sap ls
  ID                                                           STATE        AGE      
  feature-population-results-aggregator                        RUNNING      21h13m24s              
  ~ sap ui -i aggregator
  Port forwarding feature-population-results-aggregator-driver to http://localhost:4040...
  ```
