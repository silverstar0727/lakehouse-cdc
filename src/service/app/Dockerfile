FROM python:3.12-slim

WORKDIR /app

# 환경변수 설정
# ENV POSTGRES_HOST=postgres-service
# ENV POSTGRES_USER=postgres
# ENV POSTGRES_PASSWORD=12341234
# ENV POSTGRES_DB=postgres
ENV PYTHONUNBUFFERED=1

# 의존성 파일 복사
COPY requirements.txt .

# 의존성 설치
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# FastAPI 서버 실행
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# 컨테이너 포트 설정
EXPOSE 8000