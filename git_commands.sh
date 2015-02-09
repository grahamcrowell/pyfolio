#!/usr/bin/env 
commit_str="data_structs.py: TimeSeriesTrans.holdings (portfolio output) \ndb_io.py: *.sql and *.db zipped in bkup"

cwd=echo pwd
git init
git status
git add "$cwd"
git commit -m "$commit_str"
# git remote add origin https://github.com/grahamcrowell/pyfolio.git
git push -u origin master
