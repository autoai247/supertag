FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 앱 코드 복사 (DB 제외)
COPY *.py ./
COPY templates/ ./templates/
COPY static/ ./static/

# 데이터 디렉토리 생성 (볼륨 마운트 포인트)
RUN mkdir -p /data/profile_pics /data/posts

# 환경변수
ENV DATA_DIR=/data
ENV DB_PATH=/data/insta.db

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
