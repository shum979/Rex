package runner

import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

/**
  * Created by Shubham Gupta on 16-Feb-18.
  * This is Driver Junit class which invokes Cucumber and triggers test execution
  */

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("classpath:features"),
  tags = Array("@wip"),
  glue = Array("classpath:steps"),
  plugin = Array("pretty", "html:target/cucumber/html",
    "json:target/cucumber/test-report.json"
  )
)
class FeatureRunnerTest {

}
