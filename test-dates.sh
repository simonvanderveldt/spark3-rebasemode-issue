#!/usr/bin/env bash
# rm -rf output/date*

# Generate testdata with 2.4.5 (no Spark version in parquet metadata)
# as well as 2.4.6 (Spark version in parquet metadata) and
# 3.0.1 (Spark version in parquet metadata + requires config for writing old dates)
# Correctly raises an exception when writing the data with Spark 3.0.1
for spark_v in 2.4.5 2.4.6 3.0.1
do
  python3 -m venv ".spark${spark_v//.}"
  source ".spark${spark_v//.}/bin/activate"
  pip install pyspark==${spark_v}
  ./generate_date_data.py
  deactivate
done
#
# # Read the data generated above using Spark 3.0.1
# # Correctly raises an exception when reading the data written with Spark 2.4.5
# source ".spark301/bin/activate"
# for spark_v in 2.4.5 2.4.6 3.0.1
# do
#   ./read_data.py "datespark${spark_v//.}"
# done
# deactivate
#
# # Read the data generated by Spark 2.4.5 using Spark 3.0.1 using the two different
# # datetimeRebaseModeInRead options
# # Should result in different dates being shown in Spark but doesn't
# source ".spark301/bin/activate"
# for rebase_mode in LEGACY CORRECTED
# do
#   ./read_data.py "datespark245" "${rebase_mode}"
# done
# deactivate

# Read the data generated by Spark 3.0.1 using Spark 3.0.1 using the two different
# datetimeRebaseModeInRead options
# Should result in the same dates being shown in Spark since it should detect how the data was written
source ".spark301/bin/activate"
for rebase_mode in LEGACY CORRECTED
do
  ./read_data.py "datespark301" "${rebase_mode}"
done
deactivate
