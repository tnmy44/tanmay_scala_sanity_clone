package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.expressions_sg.config._
import org.main.scla_dep_mgmt_change.graph.expressions_sg.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object expressions_sg {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_REF_DONOT_CHANGE = REF_DONOT_CHANGE(context, in0).interim(
      "expressions_sg",
      "TTOWjrnIg1fd9K_lp12Jl$$N7zzFGxAdOdvn3xaYBmYK",
      "WCBfclRvqMrrGgLXlPFSM$$B9llWnTIN2Ci7cV0jucao"
    )
    val df_SQL_DONOT_CHANGE = SQL_DONOT_CHANGE(context, in0).interim(
      "expressions_sg",
      "kOIAa7KrZNtDA3rc__P61$$XD2Bd0nJBnXBGO566gM4V",
      "VWxsvkKRBSLUAqVv_kOZ-$$T_LMSo9QD__0f1nqeP7KE"
    )
    val df_Join_1 =
      Join_1(context, df_REF_DONOT_CHANGE, df_SQL_DONOT_CHANGE).interim(
        "expressions_sg",
        "n1lcDGx8VC5GRkw1L2mJe$$9m886iPaSYqFd43oDP1_7",
        "2vrpj24FxoLGDEYCVRT3u$$MHreVv6tu0RLO_0VDC-tY"
      )
    val df_Limit_1 = Limit_1(context, df_Join_1).interim(
      "expressions_sg",
      "4YHcNqQ_z_gOOILNqmSFP$$0q86xV-5XkUNdZC6CvoAO",
      "NGoJ34upvgON68LeHPWF9$$YbRa2TO1l9yYYGkEAzHib"
    )
    df_Limit_1
  }

}
