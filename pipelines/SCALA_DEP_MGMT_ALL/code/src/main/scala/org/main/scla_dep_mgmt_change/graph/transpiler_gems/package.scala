package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.graph.transpiler_gems.config._
import org.main.scla_dep_mgmt_change.graph.transpiler_gems.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object transpiler_gems {

  def apply(context: Context): Unit = {
    val df_cred = cred(context).interim(
      "transpiler_gems",
      "D22FzBUpi3FoeCCGHhAxv$$hRAM8az49P8D3KnxcEcdi",
      "kXKk3a1Nk3GTOoj5Ti3Gt$$KqC16a8D7nQ9y1Dd_3KDV"
    )
    val df_normalize_data = normalize_data(context, df_cred).interim(
      "transpiler_gems",
      "dfMpsR4gcyKw6CqAzpRkA$$wPLJRF5-6WPUhUphSnD2U",
      "nf8TkVoQhovtv8XJ8KxWl$$kXYbnKfpJuGMdapoGOhXt"
    )
    val df_CompareRecords_1 =
      CompareRecords_1(context, df_cred, df_cred).interim(
        "transpiler_gems",
        "D8UMuCE6PtkGe9pjVP1rk$$wt-G3nohmQBduUUUz-dLQ",
        "tj0AtDwORWFsFJHg_Xfha$$TpVFv_rnpXjjCZpFetoO4"
      )
    df_CompareRecords_1.cache().count()
    df_CompareRecords_1.unpersist()
    val df_csv1 = csv1(context).interim(
      "transpiler_gems",
      "yXGSTfjKBU_TFuC6FtR8G$$fJy6Sh1ocmoID_R7uejwB",
      "EKkV5a8E3OzeeWE5NuZM4$$InUSGDsfPPSr34j5245-2"
    )
    val df_meta_pivot_by_seq = meta_pivot_by_seq(context, df_csv1).interim(
      "transpiler_gems",
      "4RCx7XcTZ3xpxrFYcFB3J$$PxhuQkxtBUIakuc8l0jeo",
      "l7NXdxU7gBnoyvpEvrjx9$$kTW1DI_IVJ9r8-hzrEb4c"
    )
    df_meta_pivot_by_seq.cache().count()
    df_meta_pivot_by_seq.unpersist()
    val df_add_sequence_column = add_sequence_column(context, df_cred).interim(
      "transpiler_gems",
      "AaP4-ses3J0SVnGekdZwQ$$H7CvOUcZgXD_z4FDUhKhq",
      "OkrduecslcSWZTIetILmP$$tSD5IBZfANnXhEQ0awPJb"
    )
    df_add_sequence_column.cache().count()
    df_add_sequence_column.unpersist()
    val df_reformat_seq = reformat_seq(context, df_cred).interim(
      "transpiler_gems",
      "oWM1jzqJVqqfx9lEYv0vR$$CHOlro8mdfL52BK_Dohyb",
      "S9tX_POY9RHiO3mKag1WV$$nV4gLlk6pjpXQMTX6fg_S"
    )
    df_reformat_seq.cache().count()
    df_reformat_seq.unpersist()
    val df_merge_multiple_files =
      merge_multiple_files(context, df_csv1).interim(
        "transpiler_gems",
        "9vx4oxzjLYd_LIdl4QtrB$$FO5bLGInXRtAITtYXPY65",
        "Q9_gYn4sMPYfEyDsBlIW4$$5Vy-8E8jTMK3DqcMbtaNm"
      )
    df_merge_multiple_files.cache().count()
    df_merge_multiple_files.unpersist()
    val df_sync_dataframe_columns =
      sync_dataframe_columns(context, df_cred).interim(
        "transpiler_gems",
        "8aZBb086-fevXwWn8LCtS$$5SExdLZOTuK_3rHkdHAlt",
        "2YmrcQ2Rf4FDXkjr5qz9p$$fhinf9hCmQ6ytntdEQaIO"
      )
    df_sync_dataframe_columns.cache().count()
    df_sync_dataframe_columns.unpersist()
    write_to_multiple_files(context, df_cred)
    val (df_RoundRobinPartition_1_out0, df_RoundRobinPartition_1_out1) = {
      val (df_RoundRobinPartition_1_out0_temp,
           df_RoundRobinPartition_1_out1_temp
      ) = RoundRobinPartition_1(context, df_cred)
      (df_RoundRobinPartition_1_out0_temp.interim(
         "transpiler_gems",
         "VdZxm_7XnCZIebDbQqxIi$$LauicTUYP0MacTj4FU8hG",
         "bZb-htV7-1RcgOmFH47DC$$xm4JZuKxX1wU7Wd24lN7i"
       ),
       df_RoundRobinPartition_1_out1_temp.interim(
         "transpiler_gems",
         "VdZxm_7XnCZIebDbQqxIi$$LauicTUYP0MacTj4FU8hG",
         "LjrCFRt-vuLLe3K7ULwHO$$znpj7qK59o3kLMgDcpCgy"
       )
      )
    }
    df_RoundRobinPartition_1_out0.cache().count()
    df_RoundRobinPartition_1_out0.unpersist()
    df_RoundRobinPartition_1_out1.cache().count()
    df_RoundRobinPartition_1_out1.unpersist()
    val df_log_generation = log_generation(context, df_cred).interim(
      "transpiler_gems",
      "CdA7x1NfED0YFis6t2QEf$$weJYS36ZCYoEQpYGWqZgz",
      "K5rXPyqIOt4JHneO7owLj$$uJNNRbgQFe9DVEDSGZPVn"
    )
    df_log_generation.cache().count()
    df_log_generation.unpersist()
    val df_Limit_1 = Limit_1(context, df_cred).interim(
      "transpiler_gems",
      "rF3YQl3gL_9VTG80fKDyV$$V_XD-dH8IMZhvIwwZbaPE",
      "fRkehwMKaA8ZuhEPLPWw7$$Tq5jBIzj-kt-59mmL4QFS"
    )
    df_Limit_1.cache().count()
    df_Limit_1.unpersist()
    val df_reformat_columns = reformat_columns(context, df_csv1).interim(
      "transpiler_gems",
      "ay02yBBDYMVHtPt1hyLB0$$I4i436TXVUaDrd8qpzBe8",
      "NHbl-ndvhIokU57bY0vSj$$Gv_H2eP5bmRJ7LbdCW8rw"
    )
    df_reformat_columns.cache().count()
    df_reformat_columns.unpersist()
    val df_mtime = mtime(context).interim(
      "transpiler_gems",
      "adWH5oHeZXda0IoJAVv7e$$zdSz2Ff6gML_-wlmilnuz",
      "s6GMqtjPYW5jMSV8bS4f8$$GqqqhhsvPf9yHQzlO3S_C"
    )
    df_mtime.cache().count()
    df_mtime.unpersist()
    val df_read_separated_values =
      read_separated_values(context, df_csv1).interim(
        "transpiler_gems",
        "iLgKgfPsKaLgzZapE2SSC$$fffO--XfVLqRaLI8JGkFs",
        "tYI_AU1hvtmBaRnfMOfWO$$o82LNToFY4BMlDqEAgYpD"
      )
    df_read_separated_values.cache().count()
    df_read_separated_values.unpersist()
    val df_assertions_check = assertions_check(context, df_cred).interim(
      "transpiler_gems",
      "jXpzdW3gW7xT43jsbXPFO$$LuIYSHuGs5h0CTiHAFooR",
      "tO07OA6DfG7AmuD1ABoug$$J6rSnK95mbqOiDgjtwg-H"
    )
    df_assertions_check.cache().count()
    df_assertions_check.unpersist()
    val df_write_csv = write_csv(context, df_normalize_data).interim(
      "transpiler_gems",
      "aOrgItt42U38B2RieE1bq$$gxrc1TRzNe9z25kkCOo8V",
      "yiEWu9wdf_lCYgURpZRIK$$4nDutSl_GHw0q5ZVSHFlx"
    )
    df_write_csv.cache().count()
    df_write_csv.unpersist()
  }

}
