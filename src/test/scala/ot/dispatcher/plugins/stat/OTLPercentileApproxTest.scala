package ot.dispatcher.plugins.stat

import org.scalatest.Matchers
import ot.dispatcher.plugins.stats.commands.{OTLPercentileApprox}
import ot.dispatcher.sdk.core.{CustomException, SimpleQuery}
import ot.dispatcher.sdk.test.CommandTest

class OTLPercentileApproxTest extends CommandTest with Matchers {

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

  test("Test 0. Percentile_approx of defined column with default value (1.0). Command: | percentile_approx random_Field") {
    val query = SimpleQuery("""random_Field""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile_approx(random_Field,1.0)":120.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Percentile_approx of defined column with default value and alias. Command: | percentile_approx random_Field as rndPercApprox") {
    val query = SimpleQuery("""random_Field as rndPercApprox""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"rndPercApprox":120.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Error: only percentile_approx command word. Command: | percentile_approx") {
    val query = SimpleQuery("""""")
    the[CustomException] thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    } should have message "[SearchId:-1] Required argument(s) field not found"
  }

  test("Test 3. Percentile_approx of defined column with defined value of percentage. Command: | percentile_approx random_Field value = 0.75") {
    val query = SimpleQuery("""random_Field value = 0.75""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile_approx(random_Field,0.75)":60.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Percentile_approx of defined column with defined value of percentage and alias. Command: | percentile_approx serialField as valuedPrcApprox value = 0.37") {
    val query = SimpleQuery("""serialField as valuedPrcApprox value = 0.37""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"valuedPrcApprox":7.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Error: negative percentage. Command: | percentile_approx random_Field value = -0.77") {
    val query = SimpleQuery("""random_Field value = -0.77""")
    an[IllegalArgumentException] should be thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    }
  }

  test("Test 6. Error: percentage > 1. Command: | percentile_approx serialField value = 8.93") {
    val query = SimpleQuery("""serialField value = 8.93""")
    an[IllegalArgumentException] should be thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    }
  }

  test("Test 7. Error: text in place of percentage value. Command: | percentile_approx random_Field value = any_approx") {
    val query = SimpleQuery("""random_Field value = any_approx""")
    an[IllegalArgumentException] should be thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    }
  }

  test("Test 8. Percentile_approx with default value and defined accuracy. Command: | percentile_approx random_Field accuracy = 100") {
    val query = SimpleQuery("""random_Field accuracy = 100""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile_approx(random_Field,1.0,100)":120.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 9. Percentile_approx with defined value and defined accuracy. Command: | percentile_approx random_Field value = 0.91 accuracy = 999") {
    val query = SimpleQuery("""random_Field value = 0.91 accuracy = 999""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"percentile_approx(random_Field,0.91,999)":110.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 10. Percentile_approx with defined value and defined accuracy, with alias. Command: | percentile_approx random_Field as prcWithBigApprox value = 0.55 accuracy = 500000") {
    val query = SimpleQuery("""random_Field as prcWithBigApprox value = 0.55 accuracy = 500000""")
    val command = new OTLPercentileApprox(query, utils)
    val actual = execute(command)
    val expected =
      """[
        |{"prcWithBigApprox":50.0}
        |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 11. Error: negative accuracy. Command: | percentile_approx serialField value = 0.84 accuracy = -8") {
    val query = SimpleQuery("""serialField value = 0.84 accuracy = -8""")
    an[IllegalArgumentException] should be thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    }
  }

  test("Test 12. Error: accuracy = 0. Command: | percentile_approx random_Field value = 0.23 accuracy = 0") {
    val query = SimpleQuery("""random_Field value = 0.23 accuracy = 0""")
    an[IllegalArgumentException] should be thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    }
  }

  test("Test 13. Error: text in place of accuracy. Command: | percentile_approx random_Field accuracy = accuracy") {
    val query = SimpleQuery("""random_Field accuracy = accuracy""")
    an[IllegalArgumentException] should be thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    }
  }

  test("Test 14. Error: percentile_approx with defined value and defined accuracy, but without field defining. Command: percentile_approx value = 0.85 accuracy = 7000") {
    val query = SimpleQuery("""value = 0.85 accuracy = 7000""")
    the[CustomException] thrownBy {
      val command = new OTLPercentileApprox(query, utils)
      execute(command)
    } should have message "[SearchId:-1] Required argument(s) field not found"
  }

}
