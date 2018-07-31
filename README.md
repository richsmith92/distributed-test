### Overview

This is implementation of a [test task](http://f.nn.lv/od/5c/8y/CH_OTP_Test_Task(1).pdf) for distributed programming using Cloud Haskell (`distributed-processs`).

### Implementation details and status

* Vector timestamps are used for message ordering
* For tests localhost on localhost, there is total consensus
* With network delays simulated, consensus fails miserably
* Failed deliveries are not resent
* ~200 lines of undocumented and abandoned code

### Test run
```
nix-shell --command ./test-run.sh
```
