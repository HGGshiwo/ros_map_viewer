#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ros_map_viewer.py — move_base/TEB 路径网页可视化（Leaflet 瓦片版）

- 地图 / 全局 costmap / 局部 costmap 按瓦片(256×256 格)经 HTTP 按需下发，
  只编码视口内被请求的瓦片，内容不变时浏览器直接命中缓存，与地图总尺寸无关
- 路径 / 机器人位姿 / move_base 状态经 WebSocket 低频推送
- Leaflet.js 本地托管于 ./static/leaflet.js（无外网也可用）

用法:
    python ros_map_viewer.py [--http-port 8080] [--ws-port 9095]
浏览器打开 http://<机器人IP>:<http-port>/
"""

import argparse
import base64
import hashlib
import json
import socket
import struct
import threading
import time
import sys
import zlib
from collections import OrderedDict

PY3 = sys.version_info[0] >= 3
if PY3:
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from socketserver import ThreadingMixIn
else:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
    from SocketServer import ThreadingMixIn

try:
    import rospy
    from nav_msgs.msg import OccupancyGrid, Path, Odometry
    from geometry_msgs.msg import PoseStamped, PoseArray
    from actionlib_msgs.msg import GoalStatusArray, GoalStatus, GoalID
    from rosgraph_msgs.msg import Log
    from move_base_msgs.msg import MoveBaseActionFeedback, MoveBaseActionResult
    import tf
    ROS_OK = True
except ImportError:
    ROS_OK = False

try:
    import numpy as np
except ImportError:
    np = None

TILE = 256          # 瓦片边长(格)
TILE_CACHE_MAX = 300

STATUS_NAMES = {
    0: u"PENDING(等待)", 1: u"ACTIVE(执行中)", 2: u"PREEMPTED(被打断)",
    3: u"SUCCEEDED(成功)", 4: u"ABORTED(失败)", 5: u"REJECTED(拒绝)",
    6: u"PREEMPTING(打断中)", 7: u"RECALLED(撤销)", 8: u"LOST(丢失)",
}


# ----------------------------------------------------------------------------
# 共享状态：layers[layer] = {raw(uint8 HxW), res, ox, oy, w, h, ver}
# ----------------------------------------------------------------------------
class SharedState(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.layers = {}       # "map"/"cg"/"cl"
        self.global_plan = []
        self.local_plan = []
        self.teb_poses = []
        self.robot = None      # [x, y, th]
        self.pose_src = "--"
        self.goal = None
        self.status = {}
        self.events = []       # move_base 事件日志（环形，最新在前）
        self.rec = {"on": False, "file": "", "topics": 0, "msgs": 0, "kb": 0}
        self.rec_subs = []     # 录制期间的订阅器
        self.rec_bag = None
        self.odom = None
        self.clients = []
        self.clients_lock = threading.Lock()

    def add_client(self, ws):
        with self.clients_lock:
            self.clients.append(ws)

    def remove_client(self, ws):
        with self.clients_lock:
            if ws in self.clients:
                self.clients.remove(ws)


STATE = SharedState()

# cost → RGBA LUT（服务端渲染瓦片用）；全局与局部用不同色系错开
def _build_cost_lut(local=False):
    lut = np.zeros((256, 4), dtype=np.uint8)
    t = np.arange(98) / 98.0
    if local:
        # 局部: 青绿 → 橙 (与全局蓝→红错开)
        lut[1:99, 0] = (0 + t * 255).astype(np.uint8)
        lut[1:99, 1] = (200 - t * 60).astype(np.uint8)
        lut[1:99, 2] = (180 - t * 120).astype(np.uint8)
        lut[1:99, 3] = (30 + t * 120).astype(np.uint8)
        lut[99:253] = [255, 140, 0, 190]     # lethal: 橙
        lut[253:255] = [255, 60, 0, 200]     # 致命: 深橙红
    else:
        # 全局: 蓝 → 红
        lut[1:99, 0] = (66 + t * 178).astype(np.uint8)
        lut[1:99, 1] = (165 - t * 98).astype(np.uint8)
        lut[1:99, 2] = (245 - t * 190).astype(np.uint8)
        lut[1:99, 3] = (30 + t * 120).astype(np.uint8)
        lut[99:253] = [244, 67, 54, 190]     # lethal: 红
        lut[253:255] = [233, 30, 99, 200]
    lut[255] = [60, 60, 60, 110]             # 未知: 半透明灰
    return lut

COST_LUT = _build_cost_lut() if np is not None else None
COST_LUT_LOCAL = _build_cost_lut(local=True) if np is not None else None


# ----------------------------------------------------------------------------
# ROS 回调
# ----------------------------------------------------------------------------
def path_to_points(msg):
    return [[p.pose.position.x, p.pose.position.y] for p in msg.poses]


def _encode_grid(msg, costmap):
    """OccupancyGrid 消息 → (raw HxW uint8, ox, oy, res, w, h)
    保持消息自身的网格与原点，不做任何变换。
    raw 第0行 = 窗口底部（ROS 标准），-1(未知) → 255"""
    if np is not None:
        arr = np.array(msg.data, dtype=np.int16)
        if costmap:
            arr = np.where(arr < 0, 255, arr)
        else:
            arr = np.where(arr < 0, 128, np.where(arr >= 90, 0, 255))
        raw = arr.astype(np.uint8).reshape(msg.info.height, msg.info.width)
    else:
        raw = bytearray(len(msg.data))
        for i, v in enumerate(msg.data):
            if costmap:
                raw[i] = 255 if v == -1 else (v & 0xFF)
            elif v == -1:
                raw[i] = 128
            elif v >= 90:
                raw[i] = 0
            else:
                raw[i] = 255
        raw = np.frombuffer(bytes(raw), dtype=np.uint8).reshape(
            msg.info.height, msg.info.width)
    return (raw, msg.info.origin.position.x, msg.info.origin.position.y,
            msg.info.resolution, msg.info.width, msg.info.height)


def cb_map(msg):
    raw, ox, oy, res, w, h = _encode_grid(msg, costmap=False)
    digest = hashlib.md5(raw.tobytes()).hexdigest()
    with STATE.lock:
        prev = STATE.layers.get("map")
        if prev and prev.get("digest") == digest:
            return
        ver = (prev["ver"] + 1) if prev else 1
        STATE.layers["map"] = {"raw": raw, "ox": ox, "oy": oy, "res": res,
                               "w": w, "h": h, "ver": ver, "digest": digest}
    _invalidate_tiles("map")
    print("[viewer] 地图更新 ver=%d (%dx%d)" % (ver, w, h))


def _invalidate_tiles(name):
    with TILE_CACHE_LOCK:
        for key in [k for k in TILE_CACHE if k[0] == name]:
            del TILE_CACHE[key]


def _make_costmap_cb(name):
    """costmap 回调：编码（保持自身网格与原点，不做任何变换）+ digest 去重。
    坐标对齐由通用瓦片渲染函数 render_tile_from_grid 按需采样完成。"""
    def cb(msg):
        now = time.time()
        if now - getattr(cb, "_t", 0) < 0.5:   # ≤2Hz
            return
        cb._t = now
        raw, ox, oy, res, w, h = _encode_grid(msg, costmap=True)
        digest = hashlib.md5(raw.tobytes()).hexdigest()
        if digest == getattr(cb, "_digest", None):
            return                       # 内容没变：不动状态
        cb._digest = digest
        with STATE.lock:
            prev = STATE.layers.get(name)
            ver = (prev["ver"] + 1) if prev else 1
            STATE.layers[name] = {"raw": raw, "ox": ox, "oy": oy, "res": res,
                                  "w": w, "h": h, "ver": ver}
        _invalidate_tiles(name)
    return cb


cb_costmap_global = _make_costmap_cb("cg")
cb_costmap_local = _make_costmap_cb("cl")


def cb_global_plan(msg):
    with STATE.lock:
        STATE.global_plan = path_to_points(msg)


def cb_local_plan(msg):
    with STATE.lock:
        STATE.local_plan = path_to_points(msg)


def cb_teb_poses(msg):
    with STATE.lock:
        STATE.teb_poses = [[p.position.x, p.position.y] for p in msg.poses]


LOG_COLORS = {1: "#1565c0", 3: "#2e7d32", 4: "#c62828", 2: "#6a1b9a",
              6: "#6a1b9a", 5: "#c62828", 7: "#6a1b9a", 8: "#c62828", 0: "#777"}
ROSOUT_COLORS = {2: "#ff9800", 4: "#c62828", 3: "#ffeb3b", 8: "#888"}  # WARN/ERROR/FATAL/DEBUG


def _log_event(text, code=None, kind="mb", color=None, source=None):
    """追加一条日志到环形缓冲（move_base 事件 + rosout WARN/ERROR，最多 60 条）"""
    if color is None:
        color = LOG_COLORS.get(int(code) if code is not None else None, "#ddd")
    entry = {"t": time.strftime("%H:%M:%S"), "text": text,
             "code": int(code) if code is not None else None,
             "color": color, "kind": kind, "source": source}
    with STATE.lock:
        STATE.events.insert(0, entry)
        del STATE.events[60:]


def cb_result(msg):
    st = msg.status
    _log_event(u"结果: %s (%s)" % (STATUS_NAMES.get(int(st.status), str(st.status)),
                                   st.goal_id.id[-8:] if st.goal_id.id else "-"),
               st.status)


def cb_feedback(msg):
    pass  # "导航中"心跳不再显示（面板日志只保留状态切换/结果/rosout告警）


# ----------------------------------------------------------------------------
# rosbag 录制
# ----------------------------------------------------------------------------
import os
EXCLUDE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "exclude.yaml")

# 导航相关节点的前缀：节点名命中任意前缀即视为导航日志来源
NAV_NODE_PREFIXES = [
    "/move_base",           # move_base 及其子节点 (TEB 等)
    "/amcl",                # 定位
    "/gmapping", "/slam", "/cartographer", "/map_server",  # 建图/地图
    "/robot_state_publisher", "/joint_state_publisher",    # TF 发布
    "/cmd_vel", "/scan", "/obstacle",                       # 控制/感知相关节点
]

_DEFAULT_CFG = """# ros_map_viewer 配置
# 录制时排除的话题
exclude_topics:
  - /loc_map

# 日志面板显示的节点来源开关（true=显示其 WARN/ERROR，false=屏蔽）
# 节点名以这里的前缀匹配
log_nodes:
  move_base: true
  amcl: true
  slam: true
  cmd_vel: true
"""


def load_config():
    """读取 exclude.yaml（兼容旧格式）；返回 (排除话题列表, 日志节点开关dict)"""
    try:
        import yaml
    except ImportError:
        print("[config] 未安装 PyYAML（pip install pyyaml）")
        return [], {}
    if not os.path.isfile(EXCLUDE_FILE):
        with open(EXCLUDE_FILE, "w") as f:
            f.write(_DEFAULT_CFG)
        print("[config] 已生成默认配置: %s" % EXCLUDE_FILE)
    with open(EXCLUDE_FILE) as f:
        cfg = yaml.safe_load(f) or {}
    return list(cfg.get("exclude_topics") or []), dict(cfg.get("log_nodes") or {})


def load_exclude_topics():
    return load_config()[0]


def load_log_node_tags():
    return load_config()[1]


def is_nav_node(node_name):
    """节点名是否属于导航相关节点"""
    node = node_name or ""
    return any(node == p or node.startswith(p + "/") or node.startswith(p)
               for p in NAV_NODE_PREFIXES)


def _rec_cb(topic):
    def cb(msg):
        try:
            with STATE.rec_bag as bag:   # Bag 支持 with 语义，自动处理索引
                bag.write(topic, msg, rospy.Time.now())
                STATE.rec["msgs"] += 1
                STATE.rec["kb"] = os.path.getsize(bag.filename) // 1024
        except Exception:
            pass
    return cb


def rec_start(bag_dir):
    if STATE.rec["on"]:
        return "已在录制中"
    import rosbag
    import rostopic
    excluded = set(load_exclude_topics())
    master = rospy.get_master()
    _, _, topic_types = master.getTopicTypes()  # XMLRPC: (code, status, [(topic,type)...])
    os.makedirs(bag_dir, exist_ok=True)
    path = os.path.join(bag_dir, time.strftime("rec_%Y%m%d_%H%M%S.bag"))
    bag = rosbag.Bag(path, "w")
    STATE.rec_bag = bag
    n = 0
    for t, ttype in topic_types:
        if t in excluded:
            continue
        try:
            cls = rostopic.get_topic_class(t, blocking=False)[0]
        except Exception:
            continue
        if cls is None:
            continue
        try:
            STATE.rec_subs.append(rospy.Subscriber(t, cls, _rec_cb(t),
                                                   queue_size=30))
            n += 1
        except Exception as e:
            print("[record] 跳过 %s: %s" % (t, e))
    STATE.rec.update({"on": True, "file": path, "topics": n,
                      "msgs": 0, "kb": 0})
    _log_event(u"开始录制: %s (%d 个话题)" % (os.path.basename(path), n), 3)
    print("[record] 开始录制 %s，话题数 %d，排除: %s" % (path, n, sorted(excluded)))
    return None


def rec_stop():
    if not STATE.rec["on"]:
        return "当前没有录制"
    for s in STATE.rec_subs:
        try:
            s.unregister()
        except Exception:
            pass
    STATE.rec_subs = []
    try:
        STATE.rec_bag.close()
    except Exception:
        pass
    info = dict(STATE.rec)
    STATE.rec.update({"on": False, "file": "", "topics": 0, "msgs": 0, "kb": 0})
    _log_event(u"结束录制: %s (%d 条消息)" %
               (os.path.basename(info["file"]), info["msgs"]), 3)
    print("[record] 结束录制 %s" % info["file"])
    return None


def cb_rosout(msg):
    """记录导航相关节点的 WARN 及以上级别日志，来源可通过 YAML tag 开关"""
    if msg.level < 4:          # DEBUG=1 INFO=2 WARN=4 ERROR=8 FATAL=16
        return
    node = msg.name or ""
    if not is_nav_node(node):
        return
    # tag 开关：节点名以哪个前缀命中，就用 YAML 里对应 tag 的布尔值
    tags = load_log_node_tags()
    for p in NAV_NODE_PREFIXES:
        if node == p or node.startswith(p + "/") or node.startswith(p):
            tag = p.lstrip("/").split("/")[0]
            if tags.get(tag, True) is False:
                return
            break
    names = {4: "WARN", 8: "ERROR", 16: "FATAL"}
    lv = names.get(msg.level, str(msg.level))
    _log_event(u"[%s][%s] %s" % (lv, node, msg.msg),
               kind="rosout", color=ROSOUT_COLORS.get(msg.level, "#ff9800"),
               source=node)


def cb_status(msg):
    if not msg.status_list:
        return
    st = msg.status_list[-1]
    # 状态切换时记录事件（如 PENDING→ACTIVE、ACTIVE→ABORTED）
    prev = getattr(cb_status, "_prev_code", None)
    if prev != int(st.status):
        _log_event(u"状态: %s" % STATUS_NAMES.get(int(st.status), str(st.status)),
                   st.status)
        cb_status._prev_code = int(st.status)
    info = {
        "code": int(st.status),
        "text": STATUS_NAMES.get(int(st.status), "UNKNOWN"),
        "goal_id": st.goal_id.id,
    }
    with STATE.lock:
        STATE.status = info


goal_pub = None
cancel_pub = None


def cancel_nav_goal():
    """向 /move_base/cancel 发布空 GoalID，取消当前所有导航目标"""
    if cancel_pub is None:
        return "未连接 ROS"
    cancel_pub.publish(GoalID())
    _log_event(u"网页取消导航目标", 1)
    print("[viewer] 取消导航目标")
    return None


def send_nav_goal(wx, wy, yaw=0.0):
    """向 /move_base_simple/goal 发布目标点（map 系），RViz 2D Nav Goal 同款。
    合法性由 move_base 自行判断。"""
    if goal_pub is None:
        return "未连接 ROS"
    msg = PoseStamped()
    msg.header.stamp = rospy.Time.now()
    msg.header.frame_id = "map"
    msg.pose.position.x = wx
    msg.pose.position.y = wy
    msg.pose.orientation.z = math.sin(yaw / 2.0)
    msg.pose.orientation.w = math.cos(yaw / 2.0)
    goal_pub.publish(msg)
    _log_event(u"网页发送目标点: (%.2f, %.2f)" % (wx, wy), 1)
    print("[viewer] 发送目标点: (%.2f, %.2f, yaw=%.2f)" % (wx, wy, yaw))
    return None


def cb_goal(msg):
    q = msg.pose.orientation
    yaw = math.atan2(2.0 * (q.w * q.z + q.x * q.y), 1.0 - 2.0 * (q.y * q.y + q.z * q.z))
    with STATE.lock:
        STATE.goal = [msg.pose.position.x, msg.pose.position.y, yaw]


def cb_odom(msg):
    q = msg.pose.pose.orientation
    yaw = quat_to_yaw(q.x, q.y, q.z, q.w)
    with STATE.lock:
        STATE.odom = [msg.pose.pose.position.x, msg.pose.pose.position.y, yaw]


def quat_to_yaw(x, y, z, w):
    siny = 2.0 * (w * z + x * y)
    cosy = 1.0 - 2.0 * (y * y + z * z)
    import math
    return math.atan2(siny, cosy)  # 必须 atan2，输出 [-π, π]；直接相除在 ±90° 会发散


def tf_thread(tf_listener, rate=20.0):
    """查询 map->base_link；TF 不可用时回落 /odom（面板会提示来源）"""
    period = 1.0 / rate
    while not rospy.is_shutdown():
        try:
            (trans, rot) = tf_listener.lookupTransform("map", "base_link", rospy.Time(0))
            with STATE.lock:
                STATE.robot = [trans[0], trans[1],
                               quat_to_yaw(rot[0], rot[1], rot[2], rot[3])]
                STATE.pose_src = "TF(map)"
        except Exception:
            with STATE.lock:
                if STATE.odom is not None:
                    STATE.robot = list(STATE.odom)
                    STATE.pose_src = "odom(回落!)"
        time.sleep(period)


def ros_node(args):
    rospy.init_node("ros_map_viewer", anonymous=True, disable_signals=True)
    tf_listener = tf.TransformListener()

    rospy.Subscriber("/map", OccupancyGrid, cb_map, queue_size=1)
    rospy.Subscriber("/move_base/global_costmap/costmap", OccupancyGrid,
                     cb_costmap_global, queue_size=1)
    rospy.Subscriber("/move_base/local_costmap/costmap", OccupancyGrid,
                     cb_costmap_local, queue_size=1)
    rospy.Subscriber("/move_base/TebLocalPlannerROS/global_plan", Path,
                     cb_global_plan, queue_size=1)
    rospy.Subscriber("/move_base/TebLocalPlannerROS/local_plan", Path,
                     cb_local_plan, queue_size=1)
    rospy.Subscriber("/move_base/TebLocalPlannerROS/teb_poses", PoseArray,
                     cb_teb_poses, queue_size=1)
    rospy.Subscriber("/move_base/status", GoalStatusArray, cb_status, queue_size=1)
    rospy.Subscriber("/move_base/result", MoveBaseActionResult, cb_result, queue_size=1)
    rospy.Subscriber("/move_base/feedback", MoveBaseActionFeedback, cb_feedback, queue_size=10)
    rospy.Subscriber("/rosout", Log, cb_rosout, queue_size=50)
    rospy.Subscriber("/move_base/current_goal", PoseStamped, cb_goal, queue_size=1)
    rospy.Subscriber("/move_base_simple/goal", PoseStamped, cb_goal, queue_size=1)

    # 网页"2D 导航目标"发布器（RViz 同款功能）
    global goal_pub, cancel_pub
    goal_pub = rospy.Publisher("/move_base_simple/goal", PoseStamped, queue_size=1)
    cancel_pub = rospy.Publisher("/move_base/cancel", GoalID, queue_size=1)
    rospy.Subscriber("/odom", Odometry, cb_odom, queue_size=1)

    threading.Thread(target=tf_thread, args=(tf_listener,), daemon=True).start()
    rospy.loginfo("ros_map_viewer: ROS 订阅就绪")
    rospy.spin()


# ----------------------------------------------------------------------------
# PNG 编码（numpy + zlib，无需 PIL）与瓦片渲染
# ----------------------------------------------------------------------------
def png_encode(rgba):
    """rgba: HxWx4 uint8 → PNG bytes"""
    h, w = rgba.shape[:2]
    stride = w * 4
    raw = np.zeros((h, stride + 1), dtype=np.uint8)
    raw[:, 1:] = rgba.reshape(h, stride)   # 每行前置 filter byte 0
    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data +
                struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))
    ihdr = struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) +
            chunk(b"IDAT", zlib.compress(raw.tobytes(), 6)) +
            chunk(b"IEND", b""))


def render_tile(name, tx, ty):
    """通用瓦片渲染：任意 map 系栅格图层 → 瓦片 PNG。
    对 map / cg / cl 三层复用。原理：
    - 瓦片空间以全局地图网格为基准（原点 ox_map/oy_map，分辨率 res_map）
    - 瓦片 (tx,ty) 每个像素 (u,v) 的世界坐标：
        x = ox_map + (tx*256 + u + 0.5) * res_map
        y = oy_map + (h_map − (ty*256 + v + 0.5)) * res_map   ← v 自顶部，y 自底部
    - 世界坐标 → 源数组行列（最近邻）：col=(x−ox_s)/res_s, row=(y−oy_s)/res_s
      原点、分辨率差异在此自然吸收；"第0行=底部"约定也在此统一处理，
      不再存在任何手工翻转"""
    with STATE.lock:
        layer = STATE.layers.get(name)
        if layer is None:
            return None
        base = STATE.layers.get("map") or layer   # 瓦片空间基准层
        b_ox, b_oy, b_res, b_h = base["ox"], base["oy"], base["res"], base["h"]
        s_ox, s_oy, s_res = layer["ox"], layer["oy"], layer["res"]
        s_w, s_h = layer["w"], layer["h"]
        raw = layer["raw"]
        is_map = (name == "map")
        lut = COST_LUT if name == "cg" else COST_LUT_LOCAL

    # 每个瓦片像素中心对应的世界坐标（向量化）。
    # Leaflet CRS.Simple: pixel_y = -lat，正纬度地图位于像素 y 负半区，
    # 因此瓦片 ty 恒为负数；lat_cells = -(ty*TILE + v + 0.5) 直接得到
    # "自底部"的正向网格行号（ty=-1 是最靠下的瓦片），无需再做 b_h 减法翻转
    lat_cells = -(ty * TILE + np.arange(TILE) + 0.5)
    us = tx * TILE + np.arange(TILE) + 0.5     # 自左侧（x 正半区，tx 恒为正）
    wy = b_oy + lat_cells * b_res
    wx = b_ox + us * b_res
    # 源数组行列（rb: 第0行 = 源底部）
    rbf = (wy - s_oy) / s_res
    cbf = (wx - s_ox) / s_res
    ri = np.clip(np.floor(rbf).astype(np.int64), 0, s_h - 1)
    ci = np.clip(np.floor(cbf).astype(np.int64), 0, s_w - 1)
    # 行/列有效性必须分轴计算再外积——混在一维向量里会互相矛盾导致全透明
    valid = ((rbf >= 0) & (rbf < s_h))[:, None] & ((cbf >= 0) & (cbf < s_w))[None, :]

    vals = raw[np.ix_(ri, ci)]                 # (TILE, TILE)
    rgba = np.zeros((TILE, TILE, 4), dtype=np.uint8)
    if is_map or np is None:
        rgba[..., 0:3] = vals[..., None]
        rgba[..., 3] = np.where(valid, 255, 0).astype(np.uint8)
    else:
        rgba = lut[vals]
        rgba[..., 3] = np.where(valid, rgba[..., 3], 0).astype(np.uint8)
    return png_encode(rgba)


TILE_CACHE = OrderedDict()   # (name, ver, tx, ty) → png bytes
TILE_CACHE_LOCK = threading.Lock()


def get_tile(name, tx, ty):
    with STATE.lock:
        ver = STATE.layers.get(name, {}).get("ver", 0)
    key = (name, ver, tx, ty)
    with TILE_CACHE_LOCK:
        if key in TILE_CACHE:
            TILE_CACHE.move_to_end(key)
            return TILE_CACHE[key]
    png = render_tile(name, tx, ty)
    if png is not None:
        with TILE_CACHE_LOCK:
            TILE_CACHE[key] = png
            while len(TILE_CACHE) > TILE_CACHE_MAX:
                TILE_CACHE.popitem(last=False)
    return png


# ----------------------------------------------------------------------------
# WebSocket（纯标准库，低频推送路径/状态/图层版本号）
# ----------------------------------------------------------------------------
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise IOError("closed")
        buf += chunk
    return buf


def ws_read_frame(sock):
    b1, b2 = ws_recv_exact(sock, 2)
    opcode = b1 & 0x0F
    masked = b2 & 0x80
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack(">H", ws_recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", ws_recv_exact(sock, 8))[0]
    mask = ws_recv_exact(sock, 4) if masked else None
    payload = ws_recv_exact(sock, length) if length else b""
    if mask:
        payload = bytes(c ^ mask[i % 4] for i, c in enumerate(payload))
    return opcode, payload


def ws_send_frame(sock, payload, opcode=0x1):
    header = bytearray([0x80 | opcode])
    n = len(payload)
    if n < 126:
        header.append(n)
    elif n < 65536:
        header.append(126)
        header += struct.pack(">H", n)
    else:
        header.append(127)
        header += struct.pack(">Q", n)
    sock.sendall(bytes(header) + payload)


def ws_send_text(sock, text):
    if PY3:
        data = text.encode("utf-8")
    else:
        data = text.encode("utf-8") if isinstance(text, unicode) else text  # noqa: F821
    sock.settimeout(10.0)
    ws_send_frame(sock, data, 0x1)
    sock.settimeout(0.2)


def ws_client_thread(sock, addr):
    print("[viewer] 客户端连接: %s" % (addr,))
    try:
        sock.settimeout(0.2)
        while True:
            try:
                opcode, payload = ws_read_frame(sock)
                if opcode == 0x8:
                    break
                if opcode == 0x9:  # ping → pong
                    ws_send_frame(sock, payload, 0xA)
            except socket.timeout:
                pass
            with STATE.lock:
                vers = {k: v["ver"] for k, v in STATE.layers.items()}
                snap = {
                    "type": "snapshot",
                    "global_plan": STATE.global_plan,
                    "local_plan": STATE.local_plan,
                    "teb_poses": STATE.teb_poses,
                    "robot": STATE.robot,
                    "pose_src": STATE.pose_src,
                    "goal": STATE.goal,
                    "status": STATE.status,
                    "events": STATE.events,
                    "rec": dict(STATE.rec),
                    "vers": vers,
                }
            ws_send_text(sock, json.dumps(snap, ensure_ascii=False))
            time.sleep(0.15)  # ~6Hz 足够路径/状态显示
    except Exception as e:
        print("[viewer] 客户端断开: %s (%s)" % (addr, e))
    finally:
        STATE.remove_client(sock)
        try:
            sock.close()
        except Exception:
            pass
        print("[viewer] 客户端断开: %s" % (addr,))


def ws_server(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(8)
    print("[viewer] WebSocket 监听 0.0.0.0:%d" % port)
    while True:
        sock, addr = srv.accept()
        try:
            sock.settimeout(5.0)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = sock.recv(4096)
                if not chunk:
                    raise IOError("handshake closed")
                data += chunk
            headers = {}
            for line in data.split(b"\r\n"):
                if b":" in line:
                    k, v = line.split(b":", 1)
                    headers[k.strip().lower().decode()] = v.strip().decode()
            key = headers.get("sec-websocket-key", "")
            accept = base64.b64encode(
                hashlib.sha1((key + GUID).encode()).digest()).decode()
            resp = ("HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                    "Sec-WebSocket-Accept: %s\r\n\r\n" % accept)
            sock.sendall(resp.encode())
            STATE.add_client(sock)
            threading.Thread(target=ws_client_thread, args=(sock, addr),
                             daemon=True).start()
        except Exception as e:
            print("[viewer] 握手失败: %s" % e)
            try:
                sock.close()
            except Exception:
                pass


# ----------------------------------------------------------------------------
# HTTP：页面 / leaflet 静态文件 / layout / 瓦片
# ----------------------------------------------------------------------------
import math
import os
import subprocess
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
BAG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rosbags")
SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")


def list_nav_scripts():
    """扫描 scripts/ 目录下的 .sh 导航脚本，取首行注释作为按钮名"""
    out = []
    if os.path.isdir(SCRIPTS_DIR):
        for f in sorted(os.listdir(SCRIPTS_DIR)):
            if not f.endswith(".sh"):
                continue
            name = f[:-3]
            try:
                with open(os.path.join(SCRIPTS_DIR, f)) as fh:
                    for line in fh:
                        line = line.strip()
                        if line.startswith("#!"):
                            continue  # 跳过 shebang
                        if line.startswith("#") and len(line) > 1:
                            name = line.lstrip("#").split("：")[0].split(":")[0].strip()
                            break
                        if line and not line.startswith("#"):
                            break
            except Exception:
                pass
            out.append({"id": f[:-3], "name": name})
    return out

MIME = {".js": "application/javascript", ".css": "text/css",
        ".png": "image/png", ".html": "text/html"}


def layout_json():
    with STATE.lock:
        out = {}
        for name, L in STATE.layers.items():
            out[name] = {k: L[k] for k in ("res", "ox", "oy", "w", "h", "ver")}
        return json.dumps(out).encode()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            path = self.path.split("?")[0]
            if path in ("/", "/index.html"):
                self._serve_file(os.path.join(STATIC_DIR, "index.html"),
                                 "text/html; charset=utf-8")
            elif path.startswith("/static/"):
                f = os.path.normpath(path[len("/static/"):])
                if ".." in f:
                    self.send_error(403); return
                self._serve_file(os.path.join(STATIC_DIR, f))
            elif path == "/navgoal":
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(self.path).query)
                try:
                    wx = float(qs["x"][0]); wy = float(qs["y"][0])
                    yaw = float(qs.get("yaw", ["0"])[0])
                except (KeyError, ValueError):
                    self.send_error(400); return
                err = send_nav_goal(wx, wy, yaw)
                body = json.dumps({"ok": err is None, "err": err or ""},
                                  ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/debug/cbinfo":
                info = {}
                for cbname, cb in (("cl", cb_costmap_local), ("cg", cb_costmap_global)):
                    info[cbname] = {
                        "收包数": getattr(cb, "_n", 0),
                        "节流丢弃": getattr(cb, "_drop", 0),
                        "内容相同跳过": getattr(cb, "_same", 0),
                        "提交数": getattr(cb, "_commits", 0),
                    }
                body = json.dumps(info).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/navgoal/cancel":
                err = cancel_nav_goal()
                body = json.dumps({"ok": err is None, "err": err or ""},
                                  ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/navpoints":
                body = json.dumps(list_nav_scripts(), ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path.startswith("/navpoint/"):
                sid = path.split("/")[-1]
                fp = os.path.join(SCRIPTS_DIR, sid + ".sh")
                if sid.startswith(".") or "/" in sid or not os.path.isfile(fp):
                    self.send_error(404); return
                subprocess.Popen(["bash", fp],
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
                _log_event(u"执行导航脚本: %s" % sid, 3)
                print("[viewer] 执行导航脚本: %s" % fp)
                body = b'{"ok": true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path.startswith("/record/"):
                action = path.split("/")[-1]
                if action == "start":
                    err = rec_start(BAG_DIR)
                elif action == "stop":
                    err = rec_stop()
                else:
                    err = "未知操作: %s" % action
                body = json.dumps({"ok": err is None, "err": err or "",
                                   "rec": STATE.rec}, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/layout":
                body = layout_json()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/debug/state":
                with STATE.lock:
                    cl = STATE.layers.get("cl")
                    info = {
                        "robot": STATE.robot,
                        "pose_src": STATE.pose_src,
                        "cl_origin": [cl["ox"], cl["oy"], cl["res"], cl["w"], cl["h"]] if cl else None,
                        "map_origin": None,
                    }
                    m = STATE.layers.get("map")
                    if m:
                        info["map_origin"] = [m["ox"], m["oy"], m["res"]]
                body = json.dumps(info).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path.startswith("/debug/tileprobe"):
                # /debug/tileprobe?tx=12&ty=11 —— 服务端切片内部状态
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(self.path).query)
                tx = int(qs.get("tx", ["0"])[0]); ty = int(qs.get("ty", ["0"])[0])
                with STATE.lock:
                    layer = STATE.layers.get("map")
                    w, h = layer["w"], layer["h"]
                    raw = layer["raw"]
                x0, x1 = tx * TILE, min(tx * TILE + TILE, w)
                ytop, ybot = ty * TILE, min(ty * TILE + TILE, h)
                sub = raw[ytop:ybot, x0:x1]
                info = {
                    "raw_shape": list(raw.shape),
                    "w_h": [w, h],
                    "slice_rows": [ytop, ybot], "slice_cols": [x0, x1],
                    "sub_shape": list(sub.shape),
                    "sub_black_px": int((sub < 200).sum()),
                    "sub_min": int(sub.min()), "sub_max": int(sub.max()),
                }
                # 进程内直接调 render_tile 并解码统计（绕过 HTTP/缓存后的真相）
                png = render_tile("map", tx, ty)
                info["render_png_len"] = len(png)
                h2 = (len(png) and 0)
                if png:
                    pos = 8; idat = b";"
                    idat = b""
                    while pos < len(png):
                        ln2 = struct.unpack(">I", png[pos:pos+4])[0]
                        tg = png[pos+4:pos+8]
                        if tg == b"IDAT":
                            idat += png[pos+8:pos+8+ln2]
                        pos += 12 + ln2
                    d = zlib.decompress(idat)
                    r2 = np.frombuffer(d, np.uint8).reshape(256, 256*4+1)[:, 1:].reshape(256, 256, 4)
                    info["render_black_px"] = int((r2[..., 0] < 200).sum())
                    info["render_alpha_min"] = int(r2[..., 3].min())
                with TILE_CACHE_LOCK:
                    info["cache_has_key"] = ("map", layer["ver"], tx, ty) in TILE_CACHE
                    info["cache_size"] = len(TILE_CACHE)
                body = json.dumps(info).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path in ("/debug/map.png", "/debug/cg.png", "/debug/cl.png"):
                name = path.split("/")[-1].split(".")[0]
                if np is None:
                    self.send_error(501); return
                with STATE.lock:
                    layer = STATE.layers.get(name)
                    if layer is None:
                        self.send_error(404); return
                    raw = layer["raw"]
                if name == "map":
                    rgba = np.dstack([raw[::-1], raw[::-1], raw[::-1],
                                      np.full(raw.shape, 255, np.uint8)])
                else:
                    rgba = COST_LUT[raw[::-1]]
                png = png_encode(rgba)
                print("[viewer] debug导出 %s: %dx%d, %d 字节" %
                      (name, raw.shape[1], raw.shape[0], len(png)))
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(png)))
                self.end_headers()
                self.wfile.write(png)
            elif path.startswith("/tile/"):
                parts = path.split("/")
                # /tile/{layer}/{z}/{x}/{y}.png
                if len(parts) != 6 or not parts[5].endswith(".png"):
                    self.send_error(404); return
                name = parts[2]
                try:
                    # /tile/{layer}/{z}/{x}/{y}.png → parts[4]=x, parts[5]=y.png
                    tx = int(parts[4]); ty = int(parts[5].split(".")[0])
                except ValueError:
                    self.send_error(404); return
                png = get_tile(name, tx, ty)
                if png is None:
                    print("[viewer] 瓦片404: %s tx=%d ty=%d" % (name, tx, ty))
                    self.send_error(404); return
                print("[viewer] 瓦片200: %s tx=%d ty=%d (%d bytes)" %
                      (name, tx, ty, len(png)))
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(png)))
                # 地图层内容基本不变可长缓存；costmap 2Hz 更新需浏览器每次回源
                self.send_header("Cache-Control",
                                 "max-age=86400" if name == "map" else "no-cache")
                self.end_headers()
                self.wfile.write(png)
            else:
                self.send_error(404)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _serve_file(self, fp, ctype=None):
        if not os.path.isfile(fp):
            self.send_error(404); return
        with open(fp, "rb") as f:
            body = f.read()
        if ctype is None:
            ctype = MIME.get(os.path.splitext(fp)[1], "application/octet-stream")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")  # 页面/脚本始终重新校验，避免旧版缓存
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


# ----------------------------------------------------------------------------
# 首页（从 static/index.html 读取；不存在则提示）
# ----------------------------------------------------------------------------
INDEX_FALLBACK = u"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>ros_map_viewer</title></head><body style="font-family:sans-serif;padding:2em">
<h3>缺少 static/index.html</h3>
<p>请把发布包中的 static/index.html 与 leaflet.js / leaflet.css 放到脚本同级的 static 目录。</p>
</body></html>"""


def ensure_index():
    fp = os.path.join(STATIC_DIR, "index.html")
    if not os.path.isfile(fp):
        with open(fp, "wb") as f:
            f.write(INDEX_FALLBACK.encode("utf-8"))


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="move_base/TEB 路径网页可视化（瓦片版）")
    ap.add_argument("--ws-port", type=int, default=9095)
    ap.add_argument("--http-port", type=int, default=8080)
    args = ap.parse_args()

    if not ROS_OK:
        print("错误: 未找到 rospy，请在 ROS 环境中运行 (source devel/setup.bash)")
        sys.exit(1)

    ensure_index()
    # ROS 订阅放后台线程，主线程跑 HTTP 服务
    threading.Thread(target=ros_node, args=(args,), daemon=True).start()
    print("[viewer] 网页: http://<本机IP>:%d/   WebSocket: ws://<本机IP>:%d/ws"
          % (args.http_port, args.ws_port))
    ThreadingHTTPServer(("0.0.0.0", args.http_port), Handler).serve_forever()


if __name__ == "__main__":
    # 先解析参数：ws_server 放后台线程，HTTP 主线程
    _ap = argparse.ArgumentParser(add_help=False)
    _ap.add_argument("--ws-port", type=int, default=9095)
    _args, _ = _ap.parse_known_args()
    threading.Thread(target=ws_server, args=(_args.ws_port,), daemon=True).start()
    main()
