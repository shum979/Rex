Feature: accelerator-recon-basic-features-test

  @wip
  Scenario: Should read and store csv file after transforming it
    Given environment for TestCase1 is ready
    When configs are given by readTransformStore.xml
    Then result should produce 5 rows
    And output should be same as specified in file expectedOutput.txt

  @wip
  Scenario:Should read,recon and store data
    Given environment for TestCase2 is ready
    When configs are given by readReconStore.xml
    Then recon should produce 1 rows at field level and 5 rows at record level
    And result record should contain ravi,delta,ravi,delta,lhs_amount,rhs_amount,null,10