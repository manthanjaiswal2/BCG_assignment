from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def perform_analyses(restrict_df: DataFrame, charges_df: DataFrame,
                     units_df: DataFrame, primary_person_df: DataFrame,
                     damages_df: DataFrame, endorse_df: DataFrame):
    
    results = {}
    
    results['analysis_1'] = analysis_1(primary_person_df)
    results['analysis_2'] = analysis_2(units_df)
    results['analysis_3'] = analysis_3(primary_person_df, units_df)
    results['analysis_4'] = analysis_4(charges_df, primary_person_df)
    results['analysis_5'] = analysis_5(primary_person_df)
    results['analysis_6'] = analysis_6(units_df, primary_person_df)
    results['analysis_7'] = analysis_7(units_df, primary_person_df)
    results['analysis_8'] = analysis_8(units_df, primary_person_df)
    results['analysis_9'] = analysis_9(units_df, damages_df)
    results['analysis_10'] = analysis_10(charges_df, units_df, primary_person_df)

    return results

def analysis_1(primary_person_df):
    male_fatalities_count = primary_person_df.filter(
        (F.col("PRSN_INJRY_SEV_ID") == "KILLED") & (F.col("PRSN_GNDR_ID") == "MALE")
    ).groupBy("CRASH_ID").count().filter(F.col("count") > 2).count()
    
    return male_fatalities_count

def analysis_2(units_df):
    two_wheelers_count = units_df.filter(F.col("VEH_BODY_STYL_ID") == "MOTORCYCLE").count()
    
    return two_wheelers_count

def analysis_3(primary_person_df, units_df):
    airbag_not_deployed_count = units_df.join(
        primary_person_df.filter((F.col("PRSN_INJRY_SEV_ID") == "KILLED") & 
                                  (F.col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")),
        ["CRASH_ID", "UNIT_NBR"]
    ).groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False).limit(5).collect()
    
    return [row["VEH_MAKE_ID"] for row in airbag_not_deployed_count]

def analysis_4(charges_df, primary_person_df):
    leaving_scene_count = charges_df.filter(F.col("CHARGE") == "LEAVING SCENE OF ACCIDENT").join(
        primary_person_df.filter(F.col("PRSN_TYPE_ID").isin(["Driver", "DRIVER OF MOTORCYCLE TYPE VEHICLE"])),
        ["CRASH_ID", "UNIT_NBR"]
    ).distinct().count()
    
    return leaving_scene_count

def analysis_5(primary_person_df):
    without_females_count = primary_person_df.filter((F.col("PRSN_GNDR_ID") != "FEMALE") & (col("PRSN_GNDR_ID") != "NA")).groupBy("DRVR_LIC_STATE_ID").count().orderBy("count", ascending=False).limit(1).collect()
    
    return without_females_count[0]

def analysis_6(units_df, primary_person_df):
    not_injured_values = ["UNKNOWN", "NA", "NOT INJURED"]
    
    injuries_count = units_df.join(
        primary_person_df.filter(~F.col("PRSN_INJRY_SEV_ID").isin(not_injured_values)),
        ["CRASH_ID", "UNIT_NBR"]
    ).groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False).collect()[2:5]
    
    return [row["VEH_MAKE_ID"] for row in injuries_count]

def analysis_7(units_df, primary_person_df):
    ethnic_user_group_count = units_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"]).groupBy("PRSN_ETHNICITY_ID", "VEH_BODY_STYL_ID").count().orderBy("count", ascending=False).dropDuplicates(["VEH_BODY_STYL_ID"]).collect()
    
    return ethnic_user_group_count

def analysis_8(units_df, primary_person_df):
    alcohol_crashes_count = units_df.filter(
        (F.col("CONTRIB_FACTR_1_ID") == "UNDER INFLUENCE - ALCOHOL") | 
        (F.col("CONTRIB_FACTR_2_ID") == "UNDER INFLUENCE - ALCOHOL")
    ).join(primary_person_df.filter(F.col("DRVR_ZIP").isNotNull()), ["CRASH_ID", "UNIT_NBR"]).groupBy("DRVR_ZIP").count().orderBy("count", ascending=False).limit(5).collect()
    
    return [row["DRVR_ZIP"] for row in alcohol_crashes_count]

def analysis_9(units_df, damages_df):
    dmag_values = ["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]
    
    damages_count = units_df.filter(
        ((F.col("VEH_DMAG_SCL_1_ID") == "NO DAMAGE") | (F.col("VEH_DMAG_SCL_2_ID") == "NO DAMAGE")) & 
        ((F.col("VEH_DMAG_SCL_1_ID").isin(dmag_values)) | (F.col("VEH_DMAG_SCL_2_ID").isin(dmag_values))) & 
        (F.col("FIN_RESP_TYPE_ID") != "NA")
    ).select(F.col("CRASH_ID")).distinct().count()
    
    return damages_count

def analysis_10(charges_df, units_df, primary_person_df):
    speeding_offenses = charges_df.filter(F.col("CHARGE").contains("SPEED"))
    
    licensed_driver_data = primary_person_df.filter(F.col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")
    
    joined_data = units_df.join(speeding_offenses, ["CRASH_ID", "UNIT_NBR"]) \
                           .join(licensed_driver_data, ["CRASH_ID", "UNIT_NBR"])
    
    top_colors = joined_data.groupBy(F.col("VEH_COLOR_ID")).count().orderBy("count", ascending=False).limit(10).select("VEH_COLOR_ID")
    
    filtered_color_data = joined_data.join(top_colors, "VEH_COLOR_ID")
    
    top_states = filtered_color_data.groupBy(F.col("VEH_LIC_STATE_ID")).count().orderBy("count", ascending=False).limit(25).select(F.col("VEH_LIC_STATE_ID"))
    
    filtered_states_data = filtered_color_data.join(top_states, "VEH_LIC_STATE_ID")
    
    top_vehicle_makes_count = filtered_states_data.groupBy(F.col("VEH_MAKE_ID")).count().orderBy("count", ascending=False).limit(5).collect()
    
    return [row["VEH_MAKE_ID"] for row in top_vehicle_makes_count]
