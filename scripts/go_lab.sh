#!/bin/bash
# 去实验室：发送多楼层导航目标点
# 用法: go_lab.sh   (由 ros_map_viewer 的 /navpoint/go_lab 端点调用)
source /home/unitree/ros_navigation_ws/src/multi_floor_navigation/install/setup.bash
rosrun multi_floor_navigation send_navigation_goal.py -147.6701814 -40.55347887 50300
