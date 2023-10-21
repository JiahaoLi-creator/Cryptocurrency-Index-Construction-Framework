def signal(*args):
    df = args[0]
    n = args[1][0]
    factor_name = args[2]

    df[factor_name] = df['close'].rolling(n, min_periods=1).mean()

    return df


def get_parameter():
    param_list = []
    n_list = [3, 5, 7, 8, 10, 13, 21, 34]
    for n in n_list:
        param_list.append([n])

    return param_list
