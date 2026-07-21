#!/usr/bin/env sh
set -eu

# 使用 Git for Windows 的 sh 启动 Maven，避免 Spark 3.3.1 在 JDK 17 下触发模块访问错误。
# 可通过 RAHA_JDK8_HOME 和 RAHA_MAVEN_HOME 覆盖默认安装路径。
JDK8_HOME_WIN="${RAHA_JDK8_HOME:-D:\\Program Files\\java\\jdk8u492-b09}"
MAVEN_HOME_WIN="${RAHA_MAVEN_HOME:-D:\\Program Files\\apache-maven-3.9.12}"

to_unix_path() {
    if command -v cygpath >/dev/null 2>&1; then
        cygpath -u "$1"
        return
    fi

    # 兼容 Git Bash 缺少 cygpath 的极少数环境，仅处理常见盘符路径。
    printf '%s\n' "$1" \
        | sed -e 's#\\#/#g' \
              -e 's#^\([A-Za-z]\):#/\L\1#'
}

JDK8_HOME="$(to_unix_path "$JDK8_HOME_WIN")"
MAVEN_HOME="$(to_unix_path "$MAVEN_HOME_WIN")"

if [ ! -d "$JDK8_HOME" ]; then
    echo "JDK 8 目录不存在：$JDK8_HOME_WIN" >&2
    exit 1
fi

if [ -x "$MAVEN_HOME/bin/mvn" ]; then
    MVN_COMMAND="$MAVEN_HOME/bin/mvn"
elif [ -f "$MAVEN_HOME/bin/mvn.cmd" ]; then
    MVN_COMMAND="$MAVEN_HOME/bin/mvn.cmd"
else
    echo "Maven 命令不存在：$MAVEN_HOME_WIN/bin/mvn" >&2
    exit 1
fi

# 当前进程内覆盖 Java 环境，确保 exec:java 也运行在 JDK 8 中。
export JAVA_HOME="$JDK8_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

# 透传所有 Maven 参数，例如 clean package 或 exec:java。
exec "$MVN_COMMAND" "$@"
