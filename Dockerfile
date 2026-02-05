FROM python:3.11-slim

# --------------------------------------------------
# Set container timezone to IST
# --------------------------------------------------
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/Asia/Kolkata /etc/localtime \
    && echo Asia/Kolkata > /etc/timezone

# --------------------------------------------------
# App setup (unchanged)
# --------------------------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]