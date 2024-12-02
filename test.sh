#!/bin/bash

# シェルスクリプトの引数を確認
if [ $# -ne 1 ]; then
    echo "Usage: $0 <argument>"
    exit 1
fi

# 引数を変数に代入
ARG=$1

# Pythonスクリプトを実行
python3 ./py/test.py "$ARG"

# 終了ステータスの確認
if [ $? -eq 0 ]; then
    echo "Python script executed successfully."
else
    echo "Python script execution failed."
fi
