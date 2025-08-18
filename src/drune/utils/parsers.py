def parse_function_string(input_string):
    """
    Parses a function string, handling named, positional, and parameter-less
    functions, returning a structured list of dictionaries.
    It also handles quoted parameters.

    Args:
        input_string (str): The string to be parsed.
        Format: "func1:name_1='param1, with comma'|func2|func3:param3"

    Returns:
        list: A list of dictionaries, each representing a function.
    """
    
    def split_with_quotes(s, delimiter):
        result = []
        in_quotes = False
        quote_char = ''
        start = 0
        for i, char in enumerate(s):
            if char in ('"', "'"):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
            if char == delimiter and not in_quotes:
                result.append(s[start:i])
                start = i + 1
        result.append(s[start:])
        return result

    def strip_quotes(value):
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            return value[1:-1]
        return value

    final_result = []
    
    function_calls = split_with_quotes(input_string, '|')

    for call in function_calls:
        call = call.strip()
        if not call:
            continue

        parts = call.split(':', 1)
        function_name = parts[0].strip()
        params_string = parts[1] if len(parts) > 1 else ""

        params_dict = {}
        if params_string:
            positional_index = 0
            param_list = split_with_quotes(params_string, ',')
            
            for param in param_list:
                param = param.strip()
                if not param:
                    continue

                if '=' in param:
                    key, value = param.split('=', 1)
                    params_dict[key.strip()] = strip_quotes(value.strip())
                else:
                    params_dict[positional_index] = strip_quotes(param.strip())
                    positional_index += 1
        
        final_result.append({
            'function': function_name,
            'params': params_dict
        })
        
    return final_result
