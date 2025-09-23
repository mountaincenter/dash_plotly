# common_cfg/flags.py
# -*- coding: utf-8 -*-
"""
common_cfg.flags: 分析パイプライン用の抑止フラグ
"""
import os

PIPELINE_NO_MANIFEST = os.getenv("PIPELINE_NO_MANIFEST") == "1"
PIPELINE_NO_S3 = os.getenv("PIPELINE_NO_S3") == "1"

# 後方互換（ノートブックが import する想定の名前）
NO_MANIFEST = PIPELINE_NO_MANIFEST
NO_S3 = PIPELINE_NO_S3