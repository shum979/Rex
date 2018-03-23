<p>This module is primarily intended for end to end (functional) testing of entire project
It is based on cucumber framework and scalatest module. </p>

**Cucumber basics elements**

<li> Feature file - contains the BDD feature definitions
<li> Step definitions - Implementation of the step definitions

**Example feature**

``` 
    @wip
     Scenario: Should read and store csv file after transforming it
       Given environment for TestCase1 is ready
       When configs are given by readTransformStore.xml
       Then result should produce 5 rows
       And output should be same as specified in file expectedOutput.txt
```

**Running configuration**
* currently code executes any feature tagged with ``@wip``

* ``Scenario`` tag is description of test case, it describes intention/functionality of test case. But technically it does not mean anything

* ``Given`` tag is used to setup basic infrastructure to get test case running properly.In current implementation `Given` identifies
 test case name. <br> Like in example feature `TestCase1` is name of test case
 
* ``When`` tag is used for providing configurations and triggering execution. In current implementation `When` provides name of 
configuration XML file

* ``Then`` tag specifies expected output for current test run. 
* ``And`` this tag is similar to `Then` tag excepts it comes after `Then`

``NOTE : Given, When, Then, And tags are implemented in StepDefinition.scala file``

**How to write new test cases**
1. write all test scenarios into a feature file. This feature file should be present at _src/test/resources/features_
2. create a directory with testcase name in `testData` directory
3. This directory should have 3 sub-directories namely `config`, `data`,`expectedData`
4. `config` directory should configuration xml
5. data needed for test case should be present in `data` directory
6. if expected data is too big, it can be written in a file and this file should be placed in `expectedData` directory

**Running functional test case**

all test cases can be run by command ``maven verify -p cucumber``

**Test Reporting**

When module is run with maven-verify command it generates test reports in HTML format.Report directory is generated with name "cucumber-html-reports"