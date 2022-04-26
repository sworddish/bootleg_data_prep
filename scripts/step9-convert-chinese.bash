echo
echo "=============================================================================="
echo "Step step9-convert-traditional-simplified-chinese"
echo "=============================================================================="
echo
source ./envs.bash
python3 $BOOTLEG_PREP_CODE_DIR/langs/convert_chinese.py --data_dir $BOOTLEG_PREP_OUTPUT_DIR --conversion t2s.json
echo "Converted!"
