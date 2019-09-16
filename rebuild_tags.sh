echo 'normalizing entities'
python normalize_entities.py;
echo 'extacting tags, calculating tf'
python extract_tags_tf.py;
echo 'building inverse index, calculating idf'
python inverse_index_idf.py;
echo 'calculating tf-idf'
python calc_tf_idf.py