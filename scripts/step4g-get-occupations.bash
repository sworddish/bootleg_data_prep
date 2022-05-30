echo
echo "=============================================================================="
echo "Step step4f-get-types-hierarchy"
echo "=============================================================================="
echo
source ./envs.bash
python3 $BOOTLEG_PREP_CODE_DIR/bootleg_data_prep/wikidata/get_person_occupations.py \
    --data $BOOTLEG_PREP_WIKIDATA_DIR \
    --out_dir wikidata_occupation_output \
    --processes $BOOTLEG_PREP_PROCESS_COUNT_MAX
