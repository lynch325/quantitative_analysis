"""SQLite/ORM 落盘的最小工具函数（新增 / 提交改动 / 删除）。

只包一层 `db.session` 提交，供少量 ORM 模型（股票池、报告订阅等）复用。
注意：提交即结束事务，调用方不要依赖未提交状态；批量写请自行聚合成一次调用，
避免每行一次 commit。
"""

from app.extensions import db


def persist_new(instance):
    db.session.add(instance)
    db.session.commit()
    return instance


def persist_changes(instance):
    db.session.commit()
    return instance


def remove_instance(instance):
    db.session.delete(instance)
    db.session.commit()
    return True
