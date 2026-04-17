# AWS Lambda container image (Python)
FROM public.ecr.aws/lambda/python:3.11

COPY function ${LAMBDA_TASK_ROOT}/function

RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/function/requirements.txt

CMD [ "function.main.handler" ]
