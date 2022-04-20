echo
echo "=============================================================================="
echo "Step step0a-download-wikidata"
echo "=============================================================================="
echo
source ./local_envs/set_my_env_vars.bash
source ./envs.bash
set +e
mkdir $BOOTLEG_PREP_DATA_DIR/wikidata/
mkdir $BOOTLEG_PREP_DATA_DIR/wikidata/raw_data/
set -e
cd $BOOTLEG_PREP_DATA_DIR/wikidata/raw_data/
aria2c -s 16 -x 16 https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
echo "This will take around an hour (or less on nvme drive)..."
pigz -d latest-all.json.gz | pv -l -s 1500000000000 >/dev/null

