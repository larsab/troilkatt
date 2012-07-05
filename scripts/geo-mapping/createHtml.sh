python findDuplicates.py gds.overlap gdsTable.csv gds.duplicates
python duplicates2html.py gds.duplicates gds-duplicates.html "Dataset Duplicates (newest first)"
#
python findSupersets.py gds.overlap gdsTable.csv gds.superset.merges gds.superset.splits gds.superset.neither
python superset2html.py gds.superset.merges gds-superset-merges.html "Dataset supersets" "Dataset supersets that merge previously published dataset"
python superset2html.py gds.superset.splits gds-superset-splits.html "Dataset supersets" "Dataset supersets that are split into later published dataset"
python superset2html.py gds.superset.neither gds-superset-neither.html "Dataset supersets" "Dataset supersets that are neither older or newwer than all subsets"
#
python findCompleteSubsets.py gds.overlap gdsTable.csv gds.completesubset.oldest gds.completesubset.newest
python subset2html.py gds.completesubset.oldest gds-complete-subset-older.html "Dataset Subsets" "Dataset that have all samples in a later published dataset"
python subset2html.py gds.completesubset.newest gds-complete-subset-newer.html "Dataset Subsets" "Dataset that have all samples in a previously published dataset"
#
python findSubsets.py gds.overlap gdsTable.csv gds.subset.older gds.subset.newer
python subset2html.py gds.subset.older gds-subset-older.html "Dataset Subsets" "Dataset where some but not all samples were includd in a later dataset"
python subset2html.py gds.subset.newer gds-subset-newer.html "Dataset Subsets" "Dataset where some but not all samples were includd in a previously published dataset"
#
#
#
python findDuplicates.py gse.overlap gseTable.csv gse.duplicates
python duplicates2html.py gse.duplicates gse-duplicates.html "Series Duplicates (newest first)"
#
python findSupersets.py gse.overlap gseTable.csv gse.superset.merges gse.superset.splits gse.superset.neither
python superset2html.py gse.superset.merges gse-superset-merges.html "Series supersets" "Series supersets that merge previously published series"
python superset2html.py gse.superset.splits gse-superset-splits.html "Series supersets" "Series supersets that are split into later published series"
python superset2html.py gse.superset.neither gse-superset-neither.html "Series supersets" "Series supersets that are neither older nor newer than all subsets"
#
python findCompleteSubsets.py gse.overlap gseTable.csv gse.completesubset.oldest gse.completesubset.newest
python subset2html.py gse.completesubset.oldest gse-complete-subset-older.html "GSE subsets" "Series that have all samples in a later published series"
python subset2html.py gse.completesubset.newest gse-complete-subset-newer.html "GSE subsets" "Series that have all samples in a previously published series"
#
python findSubsets.py gse.overlap gseTable.csv gse.subset.older gse.subset.newer
python subset2html.py gse.subset.older gse-subset-older.html "Series subsets" "Series where some but not all samples were includd in a later series"
python subset2html.py gse.subset.newer gse-subset-newer.html "Series subsets" "Series where some but not all samples were includd in a previously published series"

