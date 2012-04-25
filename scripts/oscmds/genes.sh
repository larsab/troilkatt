#
# Download gene_info files
#
# Human
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Homo_sapiens.gene_info
# Mouse  
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Mus_musculus.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Mus_musculus.gene_info  
# Rat
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Rattus_norvegicus.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Rattus_norvegicus.gene_info
# Yeast
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Fungi/Saccharomyces_cerevisiae.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Saccharomyces_cerevisiae.gene_info
# Arabidopsis
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Plants/Arabidopsis_thaliana.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Arabidopsis_thaliana.gene_info
# Frog
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Non-mammalian_vertebrates/Xenopus_laevis.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Xenopus_laevis.gene_info
# Zebrafish
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Non-mammalian_vertebrates/Danio_rerio.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Danio_rerio.gene_info
# Slime mold
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Protozoa/All_Protozoa.gene_info.gz | gunzip -c > TROILKATT.META_DIR/All_Protozoa.gene_info
# Worm
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Caenorhabditis_elegans.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Caenorhabditis_elegans.gene_info
# Fly
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Drosophila_melanogaster.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Drosophila_melanogaster.gene_info
# Fission yeast
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Fungi/All_Fungi.gene_info.gz | gunzip -c > TROILKATT.META_DIR/All_Fungi.gene_info
# Dog
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Canis_familiaris.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Canis_familiaris.gene_info
# Cow
wget -q -O - ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Bos_taurus.gene_info.gz | gunzip -c > TROILKATT.META_DIR/Bos_taurus.gene_info

# Download Uniprot files
#
wget -q -O - ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping_selected.tab.gz | gunzip -c > TROILKATT.META_DIR/idmapping_selected.tab
# 
#
# Parse downloaded files to create a gene name map files for IMP 
# Human
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k hsa > TROILKATT.LOG_DIR/parseUniProtHuman.out 2> TROILKATT.LOG_DIR/parseUniProtHuman.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Homo_sapiens.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k hsa -a TROILKATT.META_DIR/hsa_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseHuman.out 2> TROILKATT.LOG_DIR/parseHuman.err
# Mouse
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k mmu > TROILKATT.LOG_DIR/parseUniProtMouse.out 2> TROILKATT.LOG_DIR/parseUniProtMouse.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Mus_musculus.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k mmu -a TROILKATT.META_DIR/mmu_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseMouse.out 2> TROILKATT.LOG_DIR/parseMouse.err
# Rat
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k rno > TROILKATT.LOG_DIR/parseUniProtRat.out 2> TROILKATT.LOG_DIR/parseUniProtRat.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Rattus_norvegicus.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k rno -a TROILKATT.META_DIR/rno_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseRat.out 2> TROILKATT.LOG_DIR/parseRat.err
# Yeast
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k sce > TROILKATT.LOG_DIR/parseUniProtYeast.out 2> TROILKATT.LOG_DIR/parseUniProtYeast.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Saccharomyces_cerevisiae.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k sce -a TROILKATT.META_DIR/sce_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseYeast.out 2> TROILKATT.LOG_DIR/parseYeast.err
# Arabidopsis
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k ath > TROILKATT.LOG_DIR/parseUniProtArabidopsis.out 2> TROILKATT.LOG_DIR/parseUniProtArabidopsis.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Arabidopsis_thaliana.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k ath -a TROILKATT.META_DIR/ath_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseArabidopsis.out 2> TROILKATT.LOG_DIR/parseArabidopsis.err
# Frog
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k xla > TROILKATT.LOG_DIR/parseUniProtFrog.out 2> TROILKATT.LOG_DIR/parseUniProtFrog.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR//Xenopus_laevis.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k xla -a TROILKATT.META_DIR/xla_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseFrog.out 2> TROILKATT.LOG_DIR/parseFrog.err
# Zebrafish
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k dre > TROILKATT.LOG_DIR/parseUniProtZebrafish.out 2> TROILKATT.LOG_DIR/parseUniProtZebrafish.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Danio_rerio.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k dre -a TROILKATT.META_DIR/dre_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseZebrafish.out 2> TROILKATT.LOG_DIR/parseZebrafish.err
# Slime mold
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k ddi > TROILKATT.LOG_DIR/parseUniProtSlimeMold.out 2> TROILKATT.LOG_DIR/parseUniProtSlimeMold.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/All_Protozoa.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k ddi -a TROILKATT.META_DIR/ddi_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseSlimeMold.out 2> TROILKATT.LOG_DIR/parseSlimeMold.err
# Worm
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k cel > TROILKATT.LOG_DIR/parseUniProtWorm.out 2> TROILKATT.LOG_DIR/parseUniProtWorm.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Caenorhabditis_elegans.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k cel -a TROILKATT.META_DIR/cel_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseWorm.out 2> TROILKATT.LOG_DIR/parseWorm.err
# Fly
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k dme > TROILKATT.LOG_DIR/parseUniProtFly.out 2> TROILKATT.LOG_DIR/parseUniProtFly.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Drosophila_melanogaster.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k dme -a TROILKATT.META_DIR/dme_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseFly.out 2> TROILKATT.LOG_DIR/parseFly.err
# Fussion Yeast
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k spo > TROILKATT.LOG_DIR/parseUniProtFissionYeast.out 2> TROILKATT.LOG_DIR/parseUniProtFussionYeast.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/All_Fungi.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k spo -a TROILKATT.META_DIR/spo_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseFussionYeast.out 2> TROILKATT.LOG_DIR/parseFussionYeast.err
# Fission Yeast
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k spo > TROILKATT.LOG_DIR/parseUniProtFissionYeast.out 2> TROILKATT.LOG_DIR/parseUniProtFissionYeast.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/All_Fungi.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k spo -a TROILKATT.META_DIR/spo_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseFissionYeast.out 2> TROILKATT.LOG_DIR/parseFissionYeast.err
# Dog
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k cfa > TROILKATT.LOG_DIR/parseUniProtDog.out 2> TROILKATT.LOG_DIR/parseUniProtDogYeast.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Canis_familiaris.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k cfa -a TROILKATT.META_DIR/spo_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseDog.out 2> TROILKATT.LOG_DIR/parseDog.err
# Cow
python TROILKATT.SCRIPTS/parseUniprotIDMapping.py -i TROILKATT.META_DIR/idmapping_selected.tab -o TROILKATT.META_DIR -k bta > TROILKATT.LOG_DIR/parseUniProtCow.out 2> TROILKATT.LOG_DIR/parseUniProtCowYeast.err 
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Bos_taurus.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/imp/ -k bta -a TROILKATT.META_DIR/spo_uniprot2entrez.tab > TROILKATT.LOG_DIR/parseCow.out 2> TROILKATT.LOG_DIR/parseCow.err


#
# Parse downloaded files to create a gene name map file for Hefalmp
#
# Human
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Homo_sapiens.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k hsa -t protein-coding > TROILKATT.LOG_DIR/parseHuman.out 2> TROILKATT.LOG_DIR/parseHuman.err
# Mouse
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Mus_musculus.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k mmu -t protein-coding > TROILKATT.LOG_DIR/parseMouse.out 2> TROILKATT.LOG_DIR/parseMouse.err
# Rat
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Rattus_norvegicus.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k rno -t protein-coding > TROILKATT.LOG_DIR/parseRat.out 2> TROILKATT.LOG_DIR/parseRat.err
# Yeast
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Saccharomyces_cerevisiae.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k sce -t protein-coding > TROILKATT.LOG_DIR/parseYeast.out 2> TROILKATT.LOG_DIR/parseYeast.err
# Arabidopsis
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Arabidopsis_thaliana.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k ath -t protein-coding > TROILKATT.LOG_DIR/parseArabidopsis.out 2> TROILKATT.LOG_DIR/parseArabidopsis.err
# Frog
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Xenopus_laevis.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k xla -t protein-coding > TROILKATT.LOG_DIR/parseFrog.out 2> TROILKATT.LOG_DIR/parseFrog.err
# Zebrafish
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Danio_rerio.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k dre -t protein-coding > TROILKATT.LOG_DIR/parseZebrafish.out 2> TROILKATT.LOG_DIR/parseZebrafish.err
# Slime mold
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/All_Protozoa.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k ddi -t protein-coding > TROILKATT.LOG_DIR/parseSlimeMold.out 2> TROILKATT.LOG_DIR/parseSlimeMold.err
# Worm
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Caenorhabditis_elegans.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k cel -t protein-coding > TROILKATT.LOG_DIR/parseWorm.out 2> TROILKATT.LOG_DIR/parseWorm.err
# Fly
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/Drosophila_melanogaster.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k dme -t protein-coding > TROILKATT.LOG_DIR/parseFly.out 2> TROILKATT.LOG_DIR/parseFly.err
# Fission yeast
python TROILKATT.SCRIPTS/parseEntrezGeneInfo.py -i TROILKATT.META_DIR/All_Fungi.gene_info -o TROILKATT.GLOBALMETA_DIR/genes/hefalmp/ -k spo -t protein-coding > TROILKATT.LOG_DIR/parseFissionYeast.out 2> TROILKATT.LOG_DIR/parseFissionYeast.err