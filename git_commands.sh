!bin
commit_str="data_structs.py, Portfolio class using functional techniques, clean CSV backups"

cwd=echo pwd
git init
git status
git add "$cwd"
git commit -m "$commit_str"
# git remote add origin https://github.com/grahamcrowell/pyfolio.git
git push -u origin master
