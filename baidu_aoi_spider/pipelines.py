import psycopg2
from shapely import wkt
from shapely.geometry import Polygon
from scrapy.exceptions import DropItem, NotConfigured
import logging

class BaiduAoiPipeline:
    def __init__(self, db):
        logging.info("BaiduAoiPipeline 初始化开始...")
        self.db = db
        try:
            self.conn = psycopg2.connect(
                dbname=self.db["database"],
                user=self.db["username"],
                password=self.db["password"],
                host=self.db["host"],
                port=self.db["port"]
            )
            self.cursor = self.conn.cursor()
            logging.info("数据库连接成功！")
        except Exception as e:
            logging.error(f"数据库连接失败: {str(e)}")
            raise
        logging.info("BaiduAoiPipeline 初始化完成")

    @classmethod
    def from_crawler(cls, crawler):
        logger = logging.getLogger(__name__)
        logger.info("from_crawler 方法被调用！")  # 新增调试日志
        try:
            db_settings = crawler.settings.getdict("DATABASE")
            if not db_settings:
                logger.error("PostgreSQL 配置未找到！")
                raise NotConfigured("PostgreSQL 配置未找到！")
            logger.info(f"读取到的数据库配置: {db_settings}")
            
            db = {
                "drivername": db_settings["drivername"],
                "host": db_settings["host"],
                "port": db_settings["port"],
                "username": db_settings["username"],
                "password": db_settings["password"],
                "database": db_settings["database"],
            }
            logger.info("正在创建 BaiduAoiPipeline 实例...")
            return cls(db)
        except Exception as e:
            logger.error(f"从爬虫创建管道实例时发生错误: {str(e)}")
            raise

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        try:
            logging.info("解析并保存几何数据到 PostGIS")
            logging.info(f"item: {item}")
            geometry_str = item.get("geometry")
            if not geometry_str:
                raise DropItem("缺少 geometry 数据，跳过")

            if "POLYGON" in geometry_str:
                wkt_geometry = geometry_str
            else:
                coordinates = [
                    tuple(map(float, coord.split(","))) 
                    for coord in geometry_str.split(";") 
                    if coord.strip()
                ]
                if len(coordinates) < 3:
                    raise ValueError("坐标点不足，无法形成多边形")
                polygon = Polygon(coordinates)
                if not polygon.is_valid:
                    raise ValueError("无效的几何形状")
                wkt_geometry = polygon.wkt

            self.cursor.execute(
                "INSERT INTO map_aoi (name, geom) VALUES (%s, ST_GeomFromText(%s, 4326))",
                (item["uid_name"], wkt_geometry)
            )
            self.conn.commit()
            logging.info(f"成功保存几何数据：{wkt_geometry}")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"处理 item 失败：{str(e)}")
            # raise DropItem(f"解析 geometry 失败: {str(e)}")
        return item