import argparse
import io
import os

import flask_admin
from flask import Flask, send_file, jsonify
from flask_admin.contrib.peewee import ModelView

import sqlite_adapter as dba

DEFAULT_DB_NAME = "hic.db"

app = Flask("human image classifier")
app.config['SECRET_KEY'] = "extremely super safe secret key"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ip", type=str, required=True)
    p.add_argument("--port", type=int, required=True)

    p.add_argument("--init", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--image-root", type=str)
    p.add_argument("--image-exts", type=str, default=".png,.jpg,.jpeg,.bmp,.gif")
    p.add_argument("--tags", type=str, required=False)
    p.add_argument("--db-path", type=str, required=False)

    p.add_argument("--debug", action="store_true")

    return p.parse_args()


def init_flask_admin():
    admin = flask_admin.Admin(app)
    admin.add_view(ModelView(dba.DatasetInfo))
    admin.add_view(ModelView(dba.Image))
    admin.add_view(ModelView(dba.Tag))
    admin.add_view(ModelView(dba.ImageToTag))


@app.route("/image/<int:image_id>")
def get_image(image_id: int):
    image_name = db.get_image(image_id).name
    image_ext = os.path.splitext(image_name)[1]
    image_path = os.path.join(db.get_dataset_info().image_root, image_name)
    with open(image_path, 'rb') as f:
        return send_file(io.BytesIO(f.read()),
                         attachment_filename=image_name,
                         mimetype=f'image/{image_ext[1:]}')


@app.route("/image/<int:image_id>/tag", methods=["GET"])
def get_tag(image_id: int):
    return jsonify(db.get_tags_by_image_id(image_id))


@app.route("/image/<int:image_id>/tag/<string:tag_name>", methods=["POST"])
def add_tag(image_id: int, tag_name: str):
    tag = db.get_tag_by_name(tag_name)
    db.add_tag_to_image(image_id, tag)
    return jsonify(f"{image_id}, {tag_name}")


@app.route("/tag")
def get_tags():
    return jsonify(db.get_tags())


if __name__ == '__main__':
    args = parse_args()

    if args.init:
        if args.tags is None or args.image_root is None:
            raise Exception(f"require IMAGE_ROOT and TAGS with --init")
        if args.db_path:
            db_path = args.db_path
        else:
            db_path = os.path.join(args.image_root, DEFAULT_DB_NAME)

        db = dba.DBAdapter(db_path)
        if args.force:
            db.drop_db()
        db.init_db(args.image_root, args.image_exts, args.tags)
    else:
        db = dba.DBAdapter(args.db_path)

    init_flask_admin()
    app.run(host=args.ip, port=args.port, debug=args.debug)
