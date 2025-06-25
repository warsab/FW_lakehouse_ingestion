# === Import custom module from Lakehouse or local package directory ===

# Option 1: Import the full module
import my_utils as utils

# Example usage
utils.some_function()

# Option 2: Import specific function directly
from my_utils import some_function
some_function()
#________________________________________________________________________
# === Setup Python import path if module is not in default directory ===

import sys

# Add custom module location to sys.path
sys.path.append("/lakehouse/default/Files/my_project_utils")

# Import your utility module
import my_utils

# Optional: List available functions and attributes
print(dir(my_utils))
#__________________________________________________________________________
# === List all functions defined in the module using inspect ===

import inspect

# Get all functions defined in the module
functions = inspect.getmembers(my_utils, predicate=inspect.isfunction)

# Display function names and object references
for func_name, func_ref in functions:
    print(f'''
    Function Name : {func_name}
    Object Ref    : {func_ref}
    ''')
