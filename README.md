# keybase-binding-NLP

## Requirements

* Output sqlite database of this project [dentropy/keybase-binding](https://github.com/dentropy/keybase-binding)

## Delete NLP table

``` bash

sqlite3 ./keybase_export.sqlite

drop table nlp_messages_t;
drop table nlp_team_messages_t;
drop table nlp_group_messages_t;
.quit

```

## Similar Project of Mine

* [dentropy/aw-experiments](https://github.com/dentropy/aw-experiments)
* [Jupyter Notebook Viewer](https://nbviewer.org/github/dentropy/aw-experiments/blob/main/Production.ipynb)

## Sources

* [dslim/bert-base-NER · Hugging Face](https://huggingface.co/dslim/bert-base-NER?text=My+name+is+Sarah+and+I+live+in+London)