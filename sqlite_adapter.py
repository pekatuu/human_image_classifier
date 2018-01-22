import json
import os
from typing import Type, List, Dict

from peewee import *

DB = SqliteDatabase(None)

TagDict = Dict[str, List[str]]


class DatasetInfo(Model):
    image_root = CharField()
    image_exts = CharField()

    class Meta:
        database = DB


class Image(Model):
    name = CharField(max_length=255, unique=True)

    def __unicode__(self):
        return self.name

    class Meta:
        database = DB


class Tag(Model):
    name = CharField(max_length=255, unique=True)
    super_name = CharField()

    def __unicode__(self):
        return f"{self.super_name}->{self.name}"

    class Meta:
        database = DB


class ImageToTag(Model):
    image = ForeignKeyField(Image, related_name="tags")
    tag = ForeignKeyField(Tag, related_name="images")

    class Meta:
        database = DB


class DBAdapter(object):
    def __init__(self, db_path: str):
        self.db_path = db_path
        DB.init(self.db_path)

    def init_db(self, image_root: str, image_exts: str, tags_path: str):
        DB.create_tables([DatasetInfo, Image, Tag, ImageToTag], safe=False)

        dataset = DatasetInfo.create(image_root=image_root, image_exts=image_exts)
        dataset.save()

        self.update_images(image_root, image_exts)
        self.init_tags(tags_path)

    def drop_db(self):
        DB.drop_tables([DatasetInfo, Image, Tag, ImageToTag])

    def init_tags(self, tags_path: str):
        with open(tags_path) as f:
            tags = json.load(f)
        self.insert_many_atomic(Tag, tags)

    def update_images(self, image_root: str, image_exts: str):
        print(f"searching image files {image_exts} in {image_root}")

        images = os.listdir(image_root)
        exts = image_exts.split(",")

        def is_valid_image(file_name):
            full_path = os.path.join(image_root, file_name)
            lower_ext = os.path.splitext(file_name)[1].lower()
            return os.path.exists(full_path) and lower_ext in exts

        images_in_dir = set([f for f in images if is_valid_image(f)])

        print(f"{len(images_in_dir)} files found")

        images_in_db = set([i.name for i in Image.select()])
        print(f"{len(images_in_db)} records found in db {self.db_path}")

        new_images = sorted(list(images_in_dir - images_in_db))
        print(f"insert {len(new_images)} images into db")
        self.insert_many_atomic(Image, [{"name": n} for n in new_images])

    def insert_many_atomic(self, model: Type[Model], data: List[dict]):
        with DB.atomic():
            for i in range(0, len(data), 100):
                model.insert_many(data[i:i + 100]).execute()

    def get_dataset_info(self) -> DatasetInfo:
        return DatasetInfo.select().limit(1)[0]

    def get_image(self, image_id) -> Image:
        return Image.get(Image.id == image_id)

    def get_image_count(self) -> int:
        return Image.select().count()

    def get_images(self):
        return Image.select()

    def get_tag_by_name(self, name: str) -> Tag:
        return Tag.get(Tag.name == name)

    def get_tags_by_image_id(self, image_id: int) -> TagDict:
        image = Image.get(Image.id == image_id)
        im2tags = image.tags
        tags = [x.tag for x in im2tags]
        return self.jsonify_tag_list(tags)

    def get_tags(self) -> TagDict:
        tags = Tag.select()
        return self.jsonify_tag_list(tags)

    def jsonify_tag_list(self, tags: List[Tag]) -> TagDict:
        ret = {}
        for tag in tags:
            if tag.super_name in ret:
                ret[tag.super_name].append(tag.name)
            else:
                ret[tag.super_name] = [tag.name]
        return ret

    def get_as(self, t: Type[Model], obj):
        if type(obj) == int:
            ret = t.get(t.id == obj)
        else:
            ret = obj
        return ret

    def add_tag_to_image(self, _image, _tag):
        image = self.get_as(Image, _image)
        tag = self.get_as(Tag, _tag)
        rel = ImageToTag.create(image=image, tag=tag)
        rel.save()
