def parse_function(string: str) -> list:
    result = []
    string = string.strip()

    if not string:
        return result
    for item in string.split('|'):
        if ':' in item:
            func, params = item.split(':', 1)
            param_list = params.split(',') if params else []
        else:
            func = item
            param_list = []
        result.append({'function': func, 'params': param_list})
    return result