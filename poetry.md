Activate poetry virtual environment in directory perfstat in VSCode

From terminal:

Change to target directory
 C:\Users\ping\Google Drive\python\py_files>cd perfstat

Activate virtual environment
 C:\Users\ping\Google Drive\python\py_files\perfstat>poetry shell

Open the current folder inside VSCode  
 C:\Users\ping\Google Drive\python\py_files\perfstat>code .  
or ctl-alt-l (see keybindings.json)  
&nbsp;

Format python file df_best_rank.py using Black  
from terminal:

black -l 79 ps2_df_best_rank.py
or ctl-alt-b (see keybindings.json) and paste file's relative path  
&nbsp;

Indent by inserting a hard space
 Alt+0+1+6+0

Find keyboard shortcuts I added
 View>Command Palette> or Ctrl + Shift + p
 Type 'keyboard shortcuts (JSON)'
 Click 'Open Keyboard Shortcuts' or Ctrl + Alt + l (NOT Open Default Keyboard ....) shows shortcuts I added
 Click 'Define Keybinding (Ctrl-k Ctrl-k)' dialog box
 Press desire key combination and enter in dialog box to search added my added shortcuts
 Example:
  ctrl+alt+l is shortcut for:
  cd perfstat
  poetry shell
  code .
}

ModuleNotFoundError: No module named 'pandas':  
After moving directory, run poetry install. The install command reads the pyproject.toml file from the current project, resolves the dependencies, and installs them.
