import sys, re
def interp(string):
    locals  = sys._getframe(1).f_locals
    globals = sys._getframe(1).f_globals
    for item in re.findall(r'#\{([^}]*)\}', string):
        string = string.replace('#{%s}' % item,
            str(eval(item, globals, locals)), 1)
    return string

def _(string):
    locals  = sys._getframe(1).f_locals
    globals = sys._getframe(1).f_globals
    for item in re.findall(r'#\{([^}]*)\}', string):
        string = string.replace('#{%s}' % item,
            str(eval(item, globals, locals)), 1)
    return string
