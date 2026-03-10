FROM python:3.12-slim

# 시스템 패키지
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리
WORKDIR /app/1_Data

# 의존성 먼저 설치 (Docker 캐시 활용)
COPY requirements.txt /app/1_Data/
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사 (개발 시에는 volume mount로 대체)
COPY . /app/1_Data/

# 환경변수
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# 기본 명령 (대시보드 실행)
CMD ["python", "-u", "p0_daily_check.py"]
