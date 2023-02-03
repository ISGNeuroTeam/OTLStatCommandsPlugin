package ot.dispatcher.plugins.stats.commands

import org.scalatest.Matchers
import ot.dispatcher.sdk.core.{CustomException, SimpleQuery}
import ot.dispatcher.sdk.test.CommandTest

class OTLPercentileTest extends CommandTest with Matchers {

  override val dataset: String =
    """[
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"0\", \"random_Field\": \"100\", \"WordField\": \"qwe\", \"junkField\": \"q2W\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188486","serialField":"0","random_Field":"100","WordField":"qwe","junkField":"q2W","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488751"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"1\", \"random_Field\": \"-90\", \"WordField\": \"rty\", \"junkField\": \"132_.\", \"second_Field\": \"bb\"}","_nifi_time":"1568037188487","serialField":"1","random_Field":"-90","WordField":"rty","junkField":"132_.","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"2\", \"random_Field\": \"50\", \"WordField\": \"uio\", \"junkField\": \"asd.cx\", \"second_Field\": \"cc\"}","_nifi_time":"1568037188487","serialField":"2","random_Field":"50","WordField":"uio","junkField":"asd.cx","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"3\", \"random_Field\": \"20\", \"WordField\": \"GreenPeace\", \"junkField\": \"XYZ\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188487","serialField":"3","random_Field":"20","WordField":"GreenPeace","junkField":"XYZ","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"4\", \"random_Field\": \"20\", \"WordField\": \"fgh\", \"junkField\": \"123_ASD\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188487","serialField":"4","random_Field":"20","WordField":"fgh","junkField":"123_ASD","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"5\", \"random_Field\": \"50\", \"WordField\": \"jkl\", \"junkField\": \"casd(@#)asd\", \"second_Field\": \"cc\"}","_nifi_time":"1568037188487","serialField":"5","random_Field":"50","WordField":"jkl","junkField":"casd(@#)asd","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"6\", \"random_Field\": \"60\", \"WordField\": \"zxc\", \"junkField\": \"QQQ.2\", \"second_Field\": \"aa\"}","_nifi_time":"1568037188487","serialField":"6","random_Field":"60","WordField":"zxc","junkField":"QQQ.2","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"7\", \"random_Field\": \"50\", \"WordField\": \"RUS\", \"junkField\": \"00_3\", \"second_Field\": \"cc\"}","_nifi_time":"1568037188487","serialField":"7","random_Field":"50","WordField":"RUS","junkField":"00_3","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"0\", \"WordField\": \"MMM\", \"junkField\": \"112\", \"second_Field\": \"dd\"}","_nifi_time":"1568037188487","serialField":"8","random_Field":"0","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"9","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"80\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"10","random_Field":"80","WordField":"USA","junkField":"RRR","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"70\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"11","random_Field":"70","WordField":"USA","junkField":"qwerty","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"110\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"12","random_Field":"110","WordField":"USA","junkField":"12334t","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"-40\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"13","random_Field":"-40","WordField":"USA","junkField":"r8u","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"8\", \"random_Field\": \"5\", \"WordField\": \"MMM\", \"junkField\": \"112\", \"second_Field\": \"dd\"}","_nifi_time":"1568037188487","serialField":"14","random_Field":"5","WordField":"MMM","junkField":"112","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"10\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"15","random_Field":"10","WordField":"USA","junkField":"word","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"50\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"16","random_Field":"50","WordField":"USA","junkField":"space","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"30\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"17","random_Field":"30","WordField":"USA","junkField":"two spieces","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"120\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"18","random_Field":"120","WordField":"USA","junkField":"one piece","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"},
      {"_time":1568026476854,"_meta":"_subsecond::.854 timestamp::none","host":"test.local:9990","sourcetype":"jmx","index":"test_index","source":"test_source","_raw":"{\"serialField\": \"9\", \"random_Field\": \"-50\", \"WordField\": \"USA\", \"junkField\": \"word\", \"second_Field\": \"ee\"}","_nifi_time":"1568037188487","serialField":"19","random_Field":"-50","WordField":"USA","junkField":"Amo","_subsecond":"854","timestamp":"none","_nifi_time_out":"1568037488752"}
      ]"""

  test ("Test 0. Percentile of defined column with default value (1.0). Command: | percentile random_Field") {
    val query = SimpleQuery("""random_Field""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile(random_Field,1.0)":120.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Percentile of defined column with default value and alias. Command: | percentile random_Field as rndPercentile") {
    val query = SimpleQuery("""random_Field as rndPercentile""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"rndPercentile":120.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Error: only percentile command word. Command: | percentile") {
    val query = SimpleQuery("""""")
    the [CustomException] thrownBy{
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "[SearchId:-1] Required argument(s) field not found"
  }

  test("Test 3. Percentile of defined column with defined value of percentage. Command: | percentile random_Field value = 0.75") {
    val query = SimpleQuery("""random_Field value = 0.75""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile(random_Field,0.75)":62.5}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Percentile of defined column with defined value of percentage and alias. Command: | percentile serialField as valuedPrc value = 0.37") {
    val query = SimpleQuery("""serialField as valuedPrc value = 0.37""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"valuedPrc":7.03}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Error: negative percentage. Command: | percentile random_Field value = -0.63") {
    val query = SimpleQuery("""random_Field value = -0.63""")
    the[IllegalArgumentException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "value should be from 0.0 to 1.0, but it is -0.63"
  }

  test("Test 6. Error: percentage > 1. Command: | percentile serialField value = 6.15") {
    val query = SimpleQuery("""serialField value = 6.15""")
    the[IllegalArgumentException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "value should be from 0.0 to 1.0, but it is 6.15"
  }

  test("Test 7. Error: text in place of percentage value. Command: | percentile random_Field value = any") {
    val query = SimpleQuery("""random_Field value = any""")
    the[IllegalArgumentException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "value should has double type, but it has other type"
  }

  test("Test 8. Percentile with default value and defined frequency. Command: | percentile random_Field frequency = 5") {
    val query = SimpleQuery("""random_Field frequency = 5""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile(random_Field,1.0,5)":120.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9. Percentile with defined value and defined frequency. Command: | percentile random_Field value = 0.91 frequency = 3") {
    val query = SimpleQuery("""random_Field value = 0.91 frequency = 3""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile(random_Field,0.91,3)":106.90000000000005}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 10. Percentile with defined value and defined frequency, with alias. Command: | percentile random_Field as prcWithFr9 value = 0.55 frequency = 9") {
    val query = SimpleQuery("""random_Field as prcWithFr9 value = 0.55 frequency = 9""")
    val command = new OTLPercentile(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"prcWithFr9":50.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 11. Error: negative frequency. Command: | percentile serialField value = 0.84 frequency = -6") {
    val query = SimpleQuery("""serialField value = 0.84 frequency = -6""")
    the[IllegalArgumentException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "frequency should be greater than 0, but it has value -6"
  }

  test("Test 12. Error: frequency = 0. Command: | percentile random_Field value = 0.23 frequency = 0") {
    val query = SimpleQuery("""random_Field value = 0.23 frequency = 0""")
    the[IllegalArgumentException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "frequency should be greater than 0, but it has value 0"
  }

  test("Test 13. Error: text in place of frequency. Command: | percentile random_Field frequency = newFr") {
    val query = SimpleQuery("""random_Field frequency = newFr""")
    the[IllegalArgumentException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "frequency should has integer type, but it has other type"
  }

  test("Test 14. Error: percentile with defined value and defined frequency, but without field defining. Command: percentile value = 0.97 frequency = 2") {
    val query = SimpleQuery("""value = 0.97 frequency = 2""")
    the[CustomException] thrownBy {
      val command = new OTLPercentile(query, utils)
      execute(command)
    } should have message "[SearchId:-1] Required argument(s) field not found"
  }

}
