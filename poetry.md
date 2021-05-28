Activate poetry virtual environment in directory perfstat  
from terminal:  

C:\Users\ping\Google Drive\python\py_files>cd perfstat  
C:\Users\ping\Google Drive\python\py_files\perfstat>poetry shell  
C:\Users\ping\Google Drive\python\py_files\perfstat>code .  
or ctl-alt-l (see keybindings.json)  
&nbsp;

Format python file df_best_rank.py using Black  
from terminal:  

black -l 79 ps2_df_best_rank.py
or ctl-alt-b (see keybindings.json) and paste file's relative path  
&nbsp;

Create keyboard shortcuts  
File > Preferences > Keyboard Shortcuts  
From Keyboard Shortcuts tab click Open Keyboard Shortcuts (JSON) icon

ModuleNotFoundError: No module named 'pandas':  
After moving directory, run poetry install. The install command reads the pyproject.toml file from the current project, resolves the dependencies, and installs them. 






