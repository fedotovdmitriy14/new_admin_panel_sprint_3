# Generated by Django 3.2 on 2022-11-25 21:36

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0003_rename_file_path_filmwork_file_path_somewhere'),
    ]

    operations = [
        migrations.RenameField(
            model_name='filmwork',
            old_name='file_path_somewhere',
            new_name='file_path',
        ),
    ]
