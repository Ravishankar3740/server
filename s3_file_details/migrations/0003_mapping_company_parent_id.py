# Generated by Django 2.2.4 on 2019-12-12 12:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('s3_file_details', '0002_auto_20191206_1604'),
    ]

    operations = [
        migrations.AddField(
            model_name='mapping',
            name='Company_Parent_id',
            field=models.CharField(default=None, max_length=100, null=True),
        ),
    ]
