"""Toy join function to showcase spark functions."""
def join_dataframes(left, right, columns_left, columns_right):
    column_expressions = []
    for i in range(len(columns_left)):
        left_column = left[columns_left[i]]
        right_column = right[columns_right[i]]
        column_expressions.append(left_column == right_column)
    return left.join(right, column_expressions)

