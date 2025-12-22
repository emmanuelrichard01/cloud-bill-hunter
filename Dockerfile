# Use a lightweight Python Linux image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first (for caching layers)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Expose the port Streamlit runs on
EXPOSE 8501

# The command to run when the container starts
# We chain the generation and the dashboard launch
CMD python src/generate_data.py && streamlit run src/dashboard.py