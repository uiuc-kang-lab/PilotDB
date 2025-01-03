pilot_query = """
SELECT AVG(CAST(ResolutionWidth AS BIGINT)) AS avg_1,
  AVG(CAST(ResolutionWidth AS BIGINT) + 1) AS avg_2,
  AVG(CAST(ResolutionWidth AS BIGINT) + 2) AS avg_3,
  AVG(CAST(ResolutionWidth AS BIGINT) + 3) AS avg_4,
  AVG(CAST(ResolutionWidth AS BIGINT) + 4) AS avg_5,
  AVG(CAST(ResolutionWidth AS BIGINT) + 5) AS avg_6,
  AVG(CAST(ResolutionWidth AS BIGINT) + 6) AS avg_7,
  AVG(CAST(ResolutionWidth AS BIGINT) + 7) AS avg_8,
  AVG(CAST(ResolutionWidth AS BIGINT) + 8) AS avg_9,
  AVG(CAST(ResolutionWidth AS BIGINT) + 9) AS avg_10,
  AVG(CAST(ResolutionWidth AS BIGINT) + 10) AS avg_11,
  AVG(CAST(ResolutionWidth AS BIGINT) + 11) AS avg_12,
  AVG(CAST(ResolutionWidth AS BIGINT) + 12) AS avg_13,
  AVG(CAST(ResolutionWidth AS BIGINT) + 13) AS avg_14,
  AVG(CAST(ResolutionWidth AS BIGINT) + 14) AS avg_15,
  AVG(CAST(ResolutionWidth AS BIGINT) + 15) AS avg_16,
  AVG(CAST(ResolutionWidth AS BIGINT) + 16) AS avg_17,
  AVG(CAST(ResolutionWidth AS BIGINT) + 17) AS avg_18,
  AVG(CAST(ResolutionWidth AS BIGINT) + 18) AS avg_19,
  AVG(CAST(ResolutionWidth AS BIGINT) + 19) AS avg_20,
  AVG(CAST(ResolutionWidth AS BIGINT) + 20) AS avg_21,
  AVG(CAST(ResolutionWidth AS BIGINT) + 21) AS avg_22,
  AVG(CAST(ResolutionWidth AS BIGINT) + 22) AS avg_23,
  AVG(CAST(ResolutionWidth AS BIGINT) + 23) AS avg_24,
  AVG(CAST(ResolutionWidth AS BIGINT) + 24) AS avg_25,
  AVG(CAST(ResolutionWidth AS BIGINT) + 25) AS avg_26,
  AVG(CAST(ResolutionWidth AS BIGINT) + 26) AS avg_27,
  AVG(CAST(ResolutionWidth AS BIGINT) + 27) AS avg_28,
  AVG(CAST(ResolutionWidth AS BIGINT) + 28) AS avg_29,
  AVG(CAST(ResolutionWidth AS BIGINT) + 29) AS avg_30,
  AVG(CAST(ResolutionWidth AS BIGINT) + 30) AS avg_31,
  AVG(CAST(ResolutionWidth AS BIGINT) + 31) AS avg_32,
  AVG(CAST(ResolutionWidth AS BIGINT) + 32) AS avg_33,
  AVG(CAST(ResolutionWidth AS BIGINT) + 33) AS avg_34,
  AVG(CAST(ResolutionWidth AS BIGINT) + 34) AS avg_35,
  AVG(CAST(ResolutionWidth AS BIGINT) + 35) AS avg_36,
  AVG(CAST(ResolutionWidth AS BIGINT) + 36) AS avg_37,
  AVG(CAST(ResolutionWidth AS BIGINT) + 37) AS avg_38,
  AVG(CAST(ResolutionWidth AS BIGINT) + 38) AS avg_39,
  AVG(CAST(ResolutionWidth AS BIGINT) + 39) AS avg_40,
  AVG(CAST(ResolutionWidth AS BIGINT) + 40) AS avg_41,
  AVG(CAST(ResolutionWidth AS BIGINT) + 41) AS avg_42,
  AVG(CAST(ResolutionWidth AS BIGINT) + 42) AS avg_43,
  AVG(CAST(ResolutionWidth AS BIGINT) + 43) AS avg_44,
  AVG(CAST(ResolutionWidth AS BIGINT) + 44) AS avg_45,
  AVG(CAST(ResolutionWidth AS BIGINT) + 45) AS avg_46,
  AVG(CAST(ResolutionWidth AS BIGINT) + 46) AS avg_47,
  AVG(CAST(ResolutionWidth AS BIGINT) + 47) AS avg_48,
  AVG(CAST(ResolutionWidth AS BIGINT) + 48) AS avg_49,
  AVG(CAST(ResolutionWidth AS BIGINT) + 49) AS avg_50,
  AVG(CAST(ResolutionWidth AS BIGINT) + 50) AS avg_51,
  AVG(CAST(ResolutionWidth AS BIGINT) + 51) AS avg_52,
  AVG(CAST(ResolutionWidth AS BIGINT) + 52) AS avg_53,
  AVG(CAST(ResolutionWidth AS BIGINT) + 53) AS avg_54,
  AVG(CAST(ResolutionWidth AS BIGINT) + 54) AS avg_55,
  AVG(CAST(ResolutionWidth AS BIGINT) + 55) AS avg_56,
  AVG(CAST(ResolutionWidth AS BIGINT) + 56) AS avg_57,
  AVG(CAST(ResolutionWidth AS BIGINT) + 57) AS avg_58,
  AVG(CAST(ResolutionWidth AS BIGINT) + 58) AS avg_59,
  AVG(CAST(ResolutionWidth AS BIGINT) + 59) AS avg_60,
  AVG(CAST(ResolutionWidth AS BIGINT) + 60) AS avg_61,
  AVG(CAST(ResolutionWidth AS BIGINT) + 61) AS avg_62,
  AVG(CAST(ResolutionWidth AS BIGINT) + 62) AS avg_63,
  AVG(CAST(ResolutionWidth AS BIGINT) + 63) AS avg_64,
  AVG(CAST(ResolutionWidth AS BIGINT) + 64) AS avg_65,
  AVG(CAST(ResolutionWidth AS BIGINT) + 65) AS avg_66,
  AVG(CAST(ResolutionWidth AS BIGINT) + 66) AS avg_67,
  AVG(CAST(ResolutionWidth AS BIGINT) + 67) AS avg_68,
  AVG(CAST(ResolutionWidth AS BIGINT) + 68) AS avg_69,
  AVG(CAST(ResolutionWidth AS BIGINT) + 69) AS avg_70,
  AVG(CAST(ResolutionWidth AS BIGINT) + 70) AS avg_71,
  AVG(CAST(ResolutionWidth AS BIGINT) + 71) AS avg_72,
  AVG(CAST(ResolutionWidth AS BIGINT) + 72) AS avg_73,
  AVG(CAST(ResolutionWidth AS BIGINT) + 73) AS avg_74,
  AVG(CAST(ResolutionWidth AS BIGINT) + 74) AS avg_75,
  AVG(CAST(ResolutionWidth AS BIGINT) + 75) AS avg_76,
  AVG(CAST(ResolutionWidth AS BIGINT) + 76) AS avg_77,
  AVG(CAST(ResolutionWidth AS BIGINT) + 77) AS avg_78,
  AVG(CAST(ResolutionWidth AS BIGINT) + 78) AS avg_79,
  AVG(CAST(ResolutionWidth AS BIGINT) + 79) AS avg_80,
  AVG(CAST(ResolutionWidth AS BIGINT) + 80) AS avg_81,
  AVG(CAST(ResolutionWidth AS BIGINT) + 81) AS avg_82,
  AVG(CAST(ResolutionWidth AS BIGINT) + 82) AS avg_83,
  AVG(CAST(ResolutionWidth AS BIGINT) + 83) AS avg_84,
  AVG(CAST(ResolutionWidth AS BIGINT) + 84) AS avg_85,
  AVG(CAST(ResolutionWidth AS BIGINT) + 85) AS avg_86,
  AVG(CAST(ResolutionWidth AS BIGINT) + 86) AS avg_87,
  AVG(CAST(ResolutionWidth AS BIGINT) + 87) AS avg_88,
  AVG(CAST(ResolutionWidth AS BIGINT) + 88) AS avg_89,
  AVG(CAST(ResolutionWidth AS BIGINT) + 89) AS avg_90,
  STDDEV(CAST(ResolutionWidth AS BIGINT)) AS std_1,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 1) AS std_2,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 2) AS std_3,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 3) AS std_4,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 4) AS std_5,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 5) AS std_6,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 6) AS std_7,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 7) AS std_8,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 8) AS std_9,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 9) AS std_10,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 10) AS std_11,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 11) AS std_12,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 12) AS std_13,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 13) AS std_14,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 14) AS std_15,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 15) AS std_16,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 16) AS std_17,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 17) AS std_18,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 18) AS std_19,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 19) AS std_20,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 20) AS std_21,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 21) AS std_22,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 22) AS std_23,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 23) AS std_24,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 24) AS std_25,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 25) AS std_26,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 26) AS std_27,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 27) AS std_28,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 28) AS std_29,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 29) AS std_30,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 30) AS std_31,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 31) AS std_32,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 32) AS std_33,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 33) AS std_34,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 34) AS std_35,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 35) AS std_36,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 36) AS std_37,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 37) AS std_38,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 38) AS std_39,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 39) AS std_40,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 40) AS std_41,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 41) AS std_42,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 42) AS std_43,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 43) AS std_44,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 44) AS std_45,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 45) AS std_46,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 46) AS std_47,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 47) AS std_48,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 48) AS std_49,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 49) AS std_50,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 50) AS std_51,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 51) AS std_52,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 52) AS std_53,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 53) AS std_54,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 54) AS std_55,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 55) AS std_56,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 56) AS std_57,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 57) AS std_58,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 58) AS std_59,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 59) AS std_60,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 60) AS std_61,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 61) AS std_62,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 62) AS std_63,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 63) AS std_64,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 64) AS std_65,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 65) AS std_66,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 66) AS std_67,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 67) AS std_68,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 68) AS std_69,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 69) AS std_70,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 70) AS std_71,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 71) AS std_72,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 72) AS std_73,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 73) AS std_74,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 74) AS std_75,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 75) AS std_76,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 76) AS std_77,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 77) AS std_78,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 78) AS std_79,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 79) AS std_80,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 80) AS std_81,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 81) AS std_82,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 82) AS std_83,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 83) AS std_84,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 84) AS std_85,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 85) AS std_86,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 86) AS std_87,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 87) AS std_88,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 88) AS std_89,
  STDDEV(CAST(ResolutionWidth AS BIGINT) + 89) AS std_90,
  COUNT(*) AS sample_size
FROM hits {sampling_method};
"""

sampling_query = """
SELECT SUM(CAST(ResolutionWidth AS BIGINT)) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 1) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 2) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 3) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 4) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 5) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 6) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 7) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 8) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 9) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 10) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 11) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 12) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 13) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 14) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 15) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 16) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 17) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 18) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 19) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 20) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 21) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 22) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 23) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 24) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 25) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 26) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 27) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 28) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 29) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 30) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 31) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 32) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 33) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 34) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 35) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 36) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 37) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 38) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 39) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 40) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 41) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 42) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 43) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 44) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 45) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 46) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 47) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 48) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 49) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 50) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 51) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 52) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 53) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 54) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 55) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 56) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 57) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 58) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 59) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 60) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 61) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 62) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 63) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 64) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 65) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 66) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 67) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 68) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 69) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 70) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 71) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 72) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 73) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 74) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 75) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 76) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 77) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 78) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 79) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 80) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 81) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 82) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 83) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 84) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 85) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 86) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 87) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 88) / {sample_rate},
  SUM(CAST(ResolutionWidth AS BIGINT) + 89) / {sample_rate}
FROM hits 
WHERE {sampling_method};
"""
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_3", "std": "std_3", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_4", "std": "std_4", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_5", "std": "std_5", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_6", "std": "std_6", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_7", "std": "std_7", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_8", "std": "std_8", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_9", "std": "std_9", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_10", "std": "std_10", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_11", "std": "std_11", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_12", "std": "std_12", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_13", "std": "std_13", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_14", "std": "std_14", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_15", "std": "std_15", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_16", "std": "std_16", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_17", "std": "std_17", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_18", "std": "std_18", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_19", "std": "std_19", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_20", "std": "std_20", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_21", "std": "std_21", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_22", "std": "std_22", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_23", "std": "std_23", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_24", "std": "std_24", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_25", "std": "std_25", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_26", "std": "std_26", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_27", "std": "std_27", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_28", "std": "std_28", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_29", "std": "std_29", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_30", "std": "std_30", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_31", "std": "std_31", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_32", "std": "std_32", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_33", "std": "std_33", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_34", "std": "std_34", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_35", "std": "std_35", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_36", "std": "std_36", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_37", "std": "std_37", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_38", "std": "std_38", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_39", "std": "std_39", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_40", "std": "std_40", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_41", "std": "std_41", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_42", "std": "std_42", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_43", "std": "std_43", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_44", "std": "std_44", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_45", "std": "std_45", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_46", "std": "std_46", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_47", "std": "std_47", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_48", "std": "std_48", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_49", "std": "std_49", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_50", "std": "std_50", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_51", "std": "std_51", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_52", "std": "std_52", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_53", "std": "std_53", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_54", "std": "std_54", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_55", "std": "std_55", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_56", "std": "std_56", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_57", "std": "std_57", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_58", "std": "std_58", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_59", "std": "std_59", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_60", "std": "std_60", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_61", "std": "std_61", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_62", "std": "std_62", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_63", "std": "std_63", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_64", "std": "std_64", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_65", "std": "std_65", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_66", "std": "std_66", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_67", "std": "std_67", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_68", "std": "std_68", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_69", "std": "std_69", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_70", "std": "std_70", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_71", "std": "std_71", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_72", "std": "std_72", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_73", "std": "std_73", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_74", "std": "std_74", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_75", "std": "std_75", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_76", "std": "std_76", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_77", "std": "std_77", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_78", "std": "std_78", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_79", "std": "std_79", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_80", "std": "std_80", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_81", "std": "std_81", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_82", "std": "std_82", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_83", "std": "std_83", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_84", "std": "std_84", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_85", "std": "std_85", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_86", "std": "std_86", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_87", "std": "std_87", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_88", "std": "std_88", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_89", "std": "std_89", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_90", "std": "std_90", "size": "sample_size"},
]

subquery_dict = []
